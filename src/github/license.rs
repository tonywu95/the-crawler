//! Licenses, at two levels.
//!
//! Repository: GitHub reports one license per repository. Before keeping a snapshot, the worker
//! requires a top-level license file whose text matches it, rather than trusting the API alone.
//!
//! File: permissive repositories often carry code under other licenses (a copied GPL file, a
//! vendored MPL library). A file that declares a license through an SPDX-License-Identifier, or
//! opens with a copyleft notice, is kept only if that license is permissive or the repository's.

/// Permissive licenses: use, change and redistribution allowed, with at most an attribution notice.
pub const PERMISSIVE: &[&str] = &[
    "MIT",
    "MIT-0",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "0BSD",
    "Unlicense",
    "CC0-1.0",
    "Zlib",
    "BSL-1.0",
];

/// Phrases (normalized, see `normalize`) that identify each license's text. The BSD and ISC
/// families share their phrase: this confirms the kind of license, not the exact variant.
const PERMISSIVE_SIGNATURES: &[(&str, &str)] = &[
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
];

/// Copyleft notices, most specific first: LGPL and AGPL notices also mention the GPL.
const COPYLEFT_SIGNATURES: &[(&str, &str)] = &[
    ("AGPL-3.0", "gnu affero general public license"),
    ("LGPL-2.1", "gnu lesser general public license"),
    ("LGPL-3.0", "gnu lesser general public license"),
    ("LGPL-2.1", "gnu library general public license"),
    ("GPL-2.0", "gnu general public license"),
    ("GPL-3.0", "gnu general public license"),
    ("MPL-2.0", "mozilla public license"),
    ("EPL-2.0", "eclipse public license"),
];

/// Whether a top-level file name looks like a license file: LICENSE, LICENCE.md, COPYING,
/// LICENSE-MIT, MIT-LICENSE.txt, UNLICENSE, ...
pub fn is_license_file(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    ["licen", "copying", "unlicen"]
        .iter()
        .any(|p| n.starts_with(p))
        || ["-licen", "_licen", ".licen"].iter().any(|p| n.contains(p))
}

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

/// The phrase that identifies a license's text, if this module knows one.
fn signature(spdx: &str) -> Option<&'static str> {
    let spdx = base_id(spdx);
    PERMISSIVE_SIGNATURES
        .iter()
        .chain(COPYLEFT_SIGNATURES)
        .find(|(id, _)| id.eq_ignore_ascii_case(spdx))
        .map(|(_, phrase)| *phrase)
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
    let Some(phrase) = signature(spdx) else {
        return Ok(&files[0]);
    };
    files
        .iter()
        .find(|(_, text)| normalize(text).contains(phrase))
        .ok_or_else(|| "text_mismatch".into())
}

/// Whether a file whose beginning is `head` may be kept in a repository licensed `repo_license`:
/// no SPDX-License-Identifier that the repository's license and the permissive ones fail to
/// satisfy, and, if `notices`, no copyleft notice other than the repository's own.
pub fn file_allowed(head: &str, repo_license: &str, notices: bool) -> bool {
    if let Some(expr) = spdx_expression(head) {
        return expression_allows(expr, repo_license);
    }
    if notices {
        let head = normalize(head);
        let own = signature(repo_license);
        if let Some((_, phrase)) = COPYLEFT_SIGNATURES
            .iter()
            .find(|(_, phrase)| head.contains(phrase))
        {
            return own == Some(*phrase);
        }
    }
    true
}

/// The license expression after "SPDX-License-Identifier:", to the end of its line.
fn spdx_expression(head: &str) -> Option<&str> {
    const TAG: &str = "spdx-license-identifier:";
    let at = head.to_ascii_lowercase().find(TAG)? + TAG.len();
    let rest = &head[at..];
    Some(rest.lines().next().unwrap_or(""))
}

/// Whether an SPDX expression ("MIT OR Apache-2.0", "Apache-2.0 WITH LLVM-exception", ...) can
/// be satisfied with permissive licenses and the repository's. An exception only adds
/// permissions, so `X WITH E` counts as X. An expression with no license ids allows anything.
fn expression_allows(expr: &str, repo_license: &str) -> bool {
    let allowed = |id: &str| {
        let id = base_id(id);
        PERMISSIVE.iter().any(|p| p.eq_ignore_ascii_case(id))
            || id.eq_ignore_ascii_case(base_id(repo_license))
    };
    let tokens = expr
        .split(|c: char| !(c.is_ascii_alphanumeric() || ".+-".contains(c)))
        .filter(|t| t.chars().any(|c| c.is_ascii_alphanumeric()));
    // Alternatives (OR) of conjunctions (AND), with AND binding tighter.
    let mut alternatives: Vec<Vec<&str>> = vec![Vec::new()];
    let (mut skip_exception, mut conjunction) = (false, false);
    for token in tokens {
        if std::mem::take(&mut skip_exception) {
            continue;
        }
        match token.to_ascii_uppercase().as_str() {
            "OR" => alternatives.push(Vec::new()),
            "AND" => conjunction = true,
            "WITH" => skip_exception = true,
            _ => alternatives.last_mut().unwrap().push(token),
        }
    }
    let ids: Vec<&str> = alternatives.iter().flatten().copied().collect();
    if ids.is_empty() {
        return true;
    }
    if conjunction && expr.contains('(') {
        // Grouping can change what AND applies to; require every license to be allowed.
        return ids.iter().all(|id| allowed(id));
    }
    alternatives
        .iter()
        .filter(|a| !a.is_empty())
        .any(|a| a.iter().all(|id| allowed(id)))
}

/// "GPL-2.0-only", "GPL-2.0-or-later" and "GPL-2.0+" are all GPL-2.0, as GitHub names it.
fn base_id(id: &str) -> &str {
    let id = id.trim().trim_end_matches('+');
    id.strip_suffix("-only")
        .or_else(|| id.strip_suffix("-or-later"))
        .unwrap_or(id)
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

    #[test]
    fn checks_spdx_headers() {
        let ok = |head: &str, repo: &str| file_allowed(head, repo, true);
        assert!(ok(
            "// SPDX-License-Identifier: MIT\nfn main() {}",
            "Apache-2.0"
        ));
        assert!(ok("# SPDX-License-Identifier: (MIT OR Apache-2.0)", "MIT"));
        assert!(ok("/* SPDX-License-Identifier: GPL-2.0 OR MIT */", "MIT"));
        assert!(ok("/* SPDX-License-Identifier: (GPL-2.0 OR MIT) */", "MIT"));
        assert!(ok(
            "// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception",
            "MIT"
        ));
        assert!(!ok("/* SPDX-License-Identifier: GPL-2.0 */\nint x;", "MIT"));
        assert!(!ok(
            "// spdx-license-identifier: MIT AND GPL-3.0-only",
            "MIT"
        ));
        assert!(!ok(
            "// SPDX-License-Identifier: (MIT OR Apache-2.0) AND GPL-2.0",
            "MIT"
        ));
        assert!(!ok(
            "// SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note",
            "MIT"
        ));
        assert!(!ok(
            "// SPDX-License-Identifier: LicenseRef-Proprietary",
            "MIT"
        ));
        // The repository's own license is fine, however the header spells it.
        assert!(ok(
            "// SPDX-License-Identifier: GPL-2.0-or-later",
            "GPL-2.0"
        ));
        assert!(ok("// SPDX-License-Identifier: ", "MIT"));
    }

    #[test]
    fn checks_copyleft_notices() {
        let gpl = "/*\n * This program is free software; you can redistribute it and/or modify\n * it under the terms of the GNU General Public License as published by";
        assert!(!file_allowed(gpl, "MIT", true));
        assert!(
            file_allowed(gpl, "MIT", false),
            "notices are only checked when asked"
        );
        assert!(file_allowed(gpl, "GPL-3.0", true));
        let lgpl = "// Licensed under the GNU Lesser General Public License, see the GNU General Public License";
        assert!(
            !file_allowed(lgpl, "GPL-3.0", true),
            "LGPL code in a GPL repository is still another license"
        );
        assert!(file_allowed(lgpl, "LGPL-3.0", true));
        assert!(file_allowed(
            "// Copyright 2024 Octo Cat. Licensed under the MIT license.",
            "MIT",
            true
        ));
    }
}
