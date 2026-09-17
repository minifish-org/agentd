//! Transport-neutral inline images. The runtime never fetches caller-supplied URLs.
use anyhow::{bail, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

const MAX_IMAGES: usize = 8;
const MAX_IMAGE_BYTES: usize = 128 * 1024;

pub(crate) fn user_content(input: &Value, source: &str) -> Result<Value> {
    let Some(images) = input.get("images") else {
        return Ok(json!(json!({"input":input,"source":source}).to_string()));
    };
    let images = images
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("input.images must be an array"))?;
    ensure!(images.len() <= MAX_IMAGES, "input.images exceeds 8 images");
    if images.is_empty() {
        return Ok(json!(json!({"input":input,"source":source}).to_string()));
    }
    let mut metadata = input.clone();
    metadata
        .as_object_mut()
        .expect("images implies an object")
        .remove("images");
    let mut parts =
        vec![json!({"type":"text", "text":json!({"input":metadata,"source":source}).to_string()})];
    for (index, image) in images.iter().enumerate() {
        let url = image
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("image url must be an inline data URL"))?;
        validate_image(url)?;
        let caption = match image.get("caption") {
            Some(caption) => caption
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("image caption must be text"))?,
            None => "",
        };
        ensure!(caption.len() <= 4096, "image caption exceeds 4096 bytes");
        parts.push(json!({"type":"text","text":format!("Image {}: {}",index+1,caption)}));
        parts.push(json!({"type":"image_url","image_url":{"url":url}}));
    }
    Ok(Value::Array(parts))
}

fn validate_image(url: &str) -> Result<()> {
    ensure!(
        url.len() <= MAX_IMAGE_BYTES.div_ceil(3) * 4 + 32,
        "inline image exceeds 128 KiB"
    );
    let (mime, encoded) = if let Some(s) = url.strip_prefix("data:image/jpeg;base64,") {
        ("jpeg", s)
    } else if let Some(s) = url.strip_prefix("data:image/png;base64,") {
        ("png", s)
    } else if let Some(s) = url.strip_prefix("data:image/webp;base64,") {
        ("webp", s)
    } else {
        bail!("images require inline base64 JPEG, PNG or WebP; remote URLs are not fetched");
    };
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| anyhow::anyhow!("invalid image base64"))?;
    ensure!(
        bytes.len() <= MAX_IMAGE_BYTES,
        "inline image exceeds 128 KiB"
    );
    let matches = match mime {
        "jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "webp" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        _ => false,
    };
    ensure!(matches, "image bytes do not match declared media type");
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn png() -> String {
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aXioAAAAASUVORK5CYII=".into()
    }
    #[test]
    fn images_are_visual_parts_not_json_text() {
        let result = user_content(
            &json!({"text":"look","images":[{"url":png(),"caption":"root post"}]}),
            "api",
        )
        .unwrap();
        assert_eq!(result[2]["type"], "image_url");
        assert_eq!(result[2]["image_url"]["url"], png());
        assert!(!result[0]["text"].as_str().unwrap().contains("base64"));
        let legacy = json!({"text":"hello"});
        assert_eq!(
            user_content(&legacy, "api").unwrap(),
            json!(json!({"input":legacy,"source":"api"}).to_string())
        );
    }
    #[test]
    fn rejects_remote_urls_invalid_images_and_unbounded_inputs() {
        for url in [
            "http://127.0.0.1/private",
            "https://example.com/a.png",
            "data:image/svg+xml;base64,PHN2Zz4=",
            "data:image/png;base64,YWJj",
        ] {
            assert!(user_content(&json!({"images":[{"url":url}]}), "api").is_err());
        }
        assert!(user_content(&json!({"images":vec![json!({"url":png()});9]}), "api").is_err());
        assert!(user_content(
            &json!({"images":[{"url":png(),"caption":"a".repeat(4097)}]}),
            "api"
        )
        .is_err());
        assert!(user_content(&json!({"images":"bad"}), "api").is_err());
    }
}
