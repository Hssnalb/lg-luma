//! "Continue watching" from the owner's Stremio library. A worker thread reads the library
//! (api.strem.io), resolves logos/backgrounds/episode names from Cinemeta and stores ready-to-draw
//! images next to the preferences, so the SDL thread only decodes small local files.
//!
//! Configuration is a `stremio.json` file next to the preferences:
//! `{"authKey": "..."}` or `{"email": "...", "password": "..."}`. No file, no row.
use image::GenericImageView as _;
use serde_json::{Value, json};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::Duration,
};

const API: &str = "https://api.strem.io/api";
const CINEMETA: &str = "https://v3-cinemeta.strem.io/meta";
const REFRESH: i64 = 5 * 60;
const MAX_ITEMS: usize = 10;
pub const THUMB: (u32, u32) = (300, 172);
const LOGO: (u32, u32) = (1040, 176);

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct Item {
    pub name: String,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub episode_title: Option<String>,
    pub offset_ms: u64,
    pub duration_ms: u64,
    pub logo: Option<String>,
    /// Logo pixel size, so Home can left-align it instead of centring.
    #[serde(default)]
    pub logo_size: (u32, u32),
    pub thumb: Option<String>,
    /// The most vivid tone of the artwork, for the label and progress bars.
    #[serde(default)]
    pub accent: Option<u32>,
    /// Stremio Web route of the episode or film, e.g. `#/detail/series/tt1/tt1%3A1%3A6`.
    #[serde(default)]
    pub route: String,
}

impl Item {
    pub fn progress(&self) -> f32 {
        if self.duration_ms == 0 {
            0.
        } else {
            (self.offset_ms as f32 / self.duration_ms as f32).clamp(0., 1.)
        }
    }
    pub fn remaining_minutes(&self) -> u64 {
        self.duration_ms
            .saturating_sub(self.offset_ms)
            .div_ceil(60_000)
    }
}

#[derive(Default)]
pub struct Resume {
    pub items: Vec<Item>,
    /// Horizontal scroll of the card row, like `State::home_scroll`.
    pub scroll: f32,
    /// The card the hero describes when focus is elsewhere.
    pub selected: usize,
    pending: bool,
    next: i64,
}

impl Resume {
    pub fn tick(&mut self, service: &Service, home_visible: bool, now: i64) -> bool {
        let mut changed = false;
        if home_visible
            && !self.pending
            && now >= self.next
            && let Some(sender) = &service.sender
            && sender.try_send(()).is_ok()
        {
            self.pending = true;
        }
        while let Ok(reply) = service.receiver.try_recv() {
            self.pending = false;
            self.next = now + REFRESH;
            // A failed refresh keeps the last list rather than emptying the row.
            if let Ok(items) = reply
                && items != self.items
            {
                self.items = items;
                self.selected = self.selected.min(self.items.len().saturating_sub(1));
                changed = true;
            }
        }
        changed
    }
}

pub struct Service {
    sender: Option<mpsc::SyncSender<()>>,
    receiver: mpsc::Receiver<Result<Vec<Item>, String>>,
}

impl Service {
    pub fn new(preferences: &Path, enabled: bool) -> Self {
        let (sender, jobs) = mpsc::sync_channel::<()>(1);
        let (results, receiver) = mpsc::sync_channel(1);
        let directory = preferences.parent().unwrap_or(Path::new(".")).to_path_buf();
        let config = directory.join("stremio.json");
        // Desktop previews only: a JSON array of `Item`s with local image paths.
        let fixture = (!cfg!(feature = "webos"))
            .then(|| std::env::var_os("LUMA_RESUME_FIXTURE"))
            .flatten();
        if !enabled || (fixture.is_none() && !config.is_file()) {
            return Self {
                sender: None,
                receiver,
            };
        }
        let cache = directory.join("resume");
        let spawned = thread::Builder::new()
            .name("stremio-resume".into())
            .spawn(move || {
                let agent = ureq::Agent::config_builder()
                    .timeout_global(Some(Duration::from_secs(10)))
                    .max_redirects(3)
                    .proxy(None)
                    .build()
                    .new_agent();
                let mut auth_key = None;
                while jobs.recv().is_ok() {
                    let result = match &fixture {
                        Some(path) => fs::read(path)
                            .map_err(|e| e.to_string())
                            .and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string())),
                        None => load(&agent, &config, &cache, &mut auth_key),
                    };
                    if let Err(error) = &result {
                        eprintln!("Luma resume: {error}");
                    }
                    if results.send(result).is_err() {
                        break;
                    }
                }
            });
        Self {
            sender: spawned.is_ok().then_some(sender),
            receiver,
        }
    }
}

fn read(response: &mut ureq::http::Response<ureq::Body>, limit: u64) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > limit {
        return Err("response too large".into());
    }
    Ok(bytes)
}

fn api(agent: &ureq::Agent, method: &str, body: Value) -> Result<Value, String> {
    let mut response = agent
        .post(format!("{API}/{method}"))
        .header("Content-Type", "application/json")
        .send(body.to_string())
        .map_err(|e| format!("Stremio {method}: {e}"))?;
    let value: Value = serde_json::from_slice(&read(&mut response, 8 << 20)?)
        .map_err(|e| format!("Stremio {method}: {e}"))?;
    if let Some(error) = value.get("error") {
        return Err(format!("Stremio {method}: {error}"));
    }
    Ok(value["result"].clone())
}

fn get_json(agent: &ureq::Agent, url: &str) -> Option<Value> {
    let mut response = agent.get(url).call().ok()?;
    serde_json::from_slice(&read(&mut response, 2 << 20).ok()?).ok()
}

fn load(
    agent: &ureq::Agent,
    config: &Path,
    cache: &Path,
    auth_key: &mut Option<String>,
) -> Result<Vec<Item>, String> {
    if auth_key.is_none() {
        let config: Value = serde_json::from_slice(&fs::read(config).map_err(|e| e.to_string())?)
            .map_err(|e| format!("stremio.json: {e}"))?;
        *auth_key = match config["authKey"].as_str() {
            Some(key) => Some(key.to_owned()),
            None => api(
                agent,
                "login",
                json!({"type": "Login", "email": config["email"], "password": config["password"], "facebook": false}),
            )?["authKey"]
                .as_str()
                .map(str::to_owned),
        };
    }
    let key = auth_key
        .clone()
        .ok_or("stremio.json needs authKey or email/password")?;
    let library = api(
        agent,
        "datastoreGet",
        json!({"authKey": key, "collection": "libraryItem", "ids": [], "all": true}),
    )
    .inspect_err(|_| *auth_key = None)?;
    let mut watching: Vec<&Value> = library
        .as_array()
        .ok_or("Stremio library is not a list")?
        .iter()
        .filter(|item| {
            item["state"]["timeOffset"].as_u64().unwrap_or(0) > 0
                && item["type"] != "other"
                && (item["removed"] != true || item["temp"] == true)
        })
        .collect();
    // ISO-8601 timestamps sort chronologically as strings.
    watching.sort_by(|a, b| {
        b["state"]["lastWatched"]
            .as_str()
            .cmp(&a["state"]["lastWatched"].as_str())
    });
    fs::create_dir_all(cache).map_err(|e| e.to_string())?;
    Ok(watching
        .into_iter()
        .take(MAX_ITEMS)
        .map(|item| resolve(agent, cache, item))
        .collect())
}

fn resolve(agent: &ureq::Agent, cache: &Path, item: &Value) -> Item {
    let id = item["_id"].as_str().unwrap_or_default();
    let kind = item["type"].as_str().unwrap_or("movie");
    let state = &item["state"];
    let video_id = state["video_id"].as_str().unwrap_or_default();
    // Series video ids are `tt123:season:episode`.
    let mut parts = video_id.rsplitn(3, ':');
    let (episode, season) = (
        parts.next().and_then(|p| p.parse().ok()),
        parts.next().and_then(|p| p.parse().ok()),
    );
    let (season, episode) = if kind == "series" {
        (season, episode)
    } else {
        (None, None)
    };
    let meta = get_json(agent, &format!("{CINEMETA}/{kind}/{id}.json"))
        .map(|value| value["meta"].clone())
        .unwrap_or(Value::Null);
    let episode_title = meta["videos"]
        .as_array()
        .and_then(|videos| videos.iter().find(|video| video["id"] == video_id))
        .and_then(|video| video["name"].as_str().or(video["title"].as_str()))
        .map(str::to_owned);
    let safe: String = id.chars().filter(char::is_ascii_alphanumeric).collect();
    let background = meta["background"].as_str().or(item["poster"].as_str());
    let thumb = background.and_then(|url| {
        image(
            agent,
            url,
            &cache.join(format!("{safe}-thumb.jpg")),
            THUMB,
            true,
        )
    });
    let logo = meta["logo"].as_str().and_then(|url| {
        image(
            agent,
            url,
            &cache.join(format!("{safe}-logo.png")),
            LOGO,
            false,
        )
    });
    let accent = thumb.as_ref().and_then(|(_, image)| accent(image));
    Item {
        name: item["name"].as_str().unwrap_or(id).to_owned(),
        season,
        episode,
        episode_title,
        offset_ms: state["timeOffset"].as_u64().unwrap_or(0),
        duration_ms: state["duration"].as_u64().unwrap_or(0),
        logo_size: logo
            .as_ref()
            .map_or((0, 0), |(_, image)| image.dimensions()),
        logo: logo.map(|(path, _)| path),
        thumb: thumb.map(|(path, _)| path),
        accent,
        route: format!(
            "#/detail/{}/{}/{}",
            encode(kind),
            encode(id),
            encode(if video_id.is_empty() { id } else { video_id })
        ),
    }
}

/// `encodeURIComponent` for ids: anything but ASCII alphanumerics and `-_.` is escaped.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Saturation-weighted mean, lifted to a readable brightness on dark glass.
fn accent(image: &image::DynamicImage) -> Option<u32> {
    let (mut sum, mut weight) = ([0f32; 3], 0f32);
    for pixel in image.to_rgb8().pixels() {
        let [r, g, b] = pixel.0.map(f32::from);
        let (max, min) = (r.max(g).max(b), r.min(g).min(b));
        let saturation = if max > 0. { (max - min) / max } else { 0. };
        let w = saturation * saturation * (max / 255.);
        sum = [sum[0] + r * w, sum[1] + g * w, sum[2] + b * w];
        weight += w;
    }
    if weight < 1. {
        return None;
    }
    let mean = sum.map(|c| c / weight);
    let lift = 230. / mean.iter().copied().fold(1., f32::max);
    let [r, g, b] = mean.map(|c| (c * lift).min(255.) as u32);
    Some(r << 16 | g << 8 | b)
}

/// Download once, then crop (cover) or fit within `size`, written atomically so the renderer
/// never caches a half-written file as missing.
fn image(
    agent: &ureq::Agent,
    url: &str,
    path: &Path,
    size: (u32, u32),
    cover: bool,
) -> Option<(String, image::DynamicImage)> {
    if let Ok(cached) = image::open(path) {
        return Some((path.to_string_lossy().into_owned(), cached));
    }
    {
        let url = if url.starts_with("//") {
            format!("https:{url}")
        } else {
            url.to_owned()
        };
        let mut response = agent.get(&url).call().ok()?;
        let decoded = image::load_from_memory(&read(&mut response, 8 << 20).ok()?).ok()?;
        let resized = if cover {
            decoded.resize_to_fill(size.0, size.1, image::imageops::FilterType::Triangle)
        } else {
            decoded.resize(size.0, size.1, image::imageops::FilterType::Triangle)
        };
        let partial = PathBuf::from(format!("{}.part", path.display()));
        let format = if cover {
            image::ImageFormat::Jpeg
        } else {
            image::ImageFormat::Png
        };
        let resized = if cover {
            image::DynamicImage::ImageRgb8(resized.into_rgb8())
        } else {
            resized
        };
        resized.save_with_format(&partial, format).ok()?;
        fs::rename(&partial, path).ok()?;
        Some((path.to_string_lossy().into_owned(), resized))
    }
}

/// "1 h 59 min" / "9 min".
pub fn duration(minutes: u64) -> String {
    match (minutes / 60, minutes % 60) {
        (0, m) => format!("{m} min"),
        (h, 0) => format!("{h} h"),
        (h, m) => format!("{h} h {m:02} min"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn progress_remaining_and_durations() {
        let item = Item {
            name: "x".into(),
            season: None,
            episode: None,
            episode_title: None,
            offset_ms: 30 * 60_000,
            duration_ms: 39 * 60_000 + 1,
            logo: None,
            logo_size: (0, 0),
            thumb: None,
            accent: None,
            route: String::new(),
        };
        assert_eq!(item.remaining_minutes(), 10);
        assert!((item.progress() - 0.769).abs() < 0.01);
        assert_eq!(duration(9), "9 min");
        assert_eq!(duration(119), "1 h 59 min");
        assert_eq!(duration(120), "2 h");
        assert_eq!(encode("tt4574334:1:6"), "tt4574334%3A1%3A6");
    }
}
