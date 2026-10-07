//! The worker's own license check, on the snapshot it is about to keep: a license file at the top
//! level whose text matches the license GitHub reported. GitHub's detection can lag the code, so
//! this requires the expected text to be present rather than trusting the API alone.

/// Whether a top-level file name looks like a license file: LICENSE, LICENCE.md, COPYING,
/// LICENSE-MIT, MIT-LICENSE.txt, UNLICENSE, ...
pub fn is_license_file(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    ["licen", "copying", "unlicen"]
        .iter()
        .any(|p| n.starts_with(p))
        || ["-licen", "_licen", ".licen"].iter().any(|p| n.contains(p))
}

/// Phrases (normalized, see `normalize`) that identify each license's text. The BSD and ISC
/// families share their phrase: this check confirms the kind of license, not the exact variant.
const SIGNATURES: &[(&str, &str)] = &[
    (
        "MIT",
        "permission is hereby granted free of charge to any person obtaining a copy",
    ),
    (
        "MIT-0",
        "permission is hereby granted free of charge to any person obtaining a copy",
    ),
    ("Apache-2.0", "apache license version 2 0"),
    (
        "BSD-2-Clause",
        "redistribution and use in source and binary forms with or without modification are permitted",
    ),
    (
        "BSD-3-Clause",
        "redistribution and use in source and binary forms with or without modification are permitted",
    ),
    (
        "ISC",
        "distribute this software for any purpose with or without fee is hereby granted",
    ),
    (
        "0BSD",
        "distribute this software for any purpose with or without fee is hereby granted",
    ),
    (
        "Unlicense",
        "this is free and unencumbered software released into the public domain",
    ),
    ("CC0-1.0", "cc0 1 0"),
    (
        "Zlib",
        "altered source versions must be plainly marked as such",
    ),
    ("BSL-1.0", "boost software license version 1 0"),
    ("MPL-2.0", "mozilla public license version 2 0"),
    ("LGPL-2.1", "gnu lesser general public license"),
    ("LGPL-3.0", "gnu lesser general public license"),
    ("GPL-2.0", "gnu general public license"),
    ("GPL-3.0", "gnu general public license"),
    ("AGPL-3.0", "gnu affero general public license"),
    ("EPL-2.0", "eclipse public license"),
    ("CC-BY-4.0", "creative commons attribution 4 0"),
];

/// Lower case, with every run of non-alphanumeric characters (punctuation, markup, line breaks)
/// turned into one space.
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for word in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        if !out.is_empty() {
            out.push(' ');
        }
        out.extend(word.chars().flat_map(char::to_lowercase));
    }
    out
}

/// Finds the license file that confirms `spdx` among `files` (name, text). For a license with no
/// known signature, any license file will do. Returns that file, or why none qualified.
pub fn verify<'a>(
    spdx: &str,
    files: &'a [(String, String)],
) -> Result<&'a (String, String), String> {
    if files.is_empty() {
        return Err("no_license_file".into());
    }
    let phrase = SIGNATURES
        .iter()
        .find(|(id, _)| id.eq_ignore_ascii_case(spdx))
        .map(|(_, p)| *p);
    let Some(phrase) = phrase else {
        return Ok(&files[0]);
    };
    files
        .iter()
        .find(|(_, text)| normalize(text).contains(phrase))
        .ok_or_else(|| "text_mismatch".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIT: &str = "MIT License\n\nCopyright (c) 2024 Octo Cat\n\nPermission is hereby granted, free of charge,\nto any person obtaining a copy of this software";
    const APACHE: &str = "\n                                 Apache License\n                           Version 2.0, January 2004";

    fn files(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn recognizes_license_file_names() {
        for name in [
            "LICENSE",
            "LICENCE.md",
            "license.txt",
            "COPYING",
            "LICENSE-MIT",
            "MIT-LICENSE.txt",
            "UNLICENSE",
        ] {
            assert!(is_license_file(name), "{name}");
        }
        for name in ["README.md", "Cargo.toml", "src", "lic.txt"] {
            assert!(!is_license_file(name), "{name}");
        }
    }

    #[test]
    fn matches_text_to_spdx() {
        let dual = files(&[("LICENSE-APACHE", APACHE), ("LICENSE-MIT", MIT)]);
        assert_eq!(verify("MIT", &dual).unwrap().0, "LICENSE-MIT");
        assert_eq!(verify("apache-2.0", &dual).unwrap().0, "LICENSE-APACHE");
        assert_eq!(verify("BSD-3-Clause", &dual), Err("text_mismatch".into()));
        assert_eq!(verify("MIT", &[]), Err("no_license_file".into()));
        // A license this module has no signature for: any license file confirms it.
        assert_eq!(verify("WTFPL", &dual).unwrap().0, "LICENSE-APACHE");
    }

    #[test]
    fn normalizes_markup_and_breaks() {
        assert_eq!(
            normalize("**Apache  License**,\r\n Version 2.0"),
            "apache license version 2 0"
        );
        assert_eq!(normalize("and/or"), "and or");
    }
}
