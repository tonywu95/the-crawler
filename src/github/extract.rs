//! Which files of a snapshot go into the dataset, file by file.
//!
//! A file is kept when it is source code, prose or small structured data in a known language,
//! written by the repository's authors (not vendored, not generated), readable text, and under
//! the repository's license. The checks run cheapest first; the first that fails names the
//! reason the file was dropped, and the manifest counts reasons per repository.

use sha1::{Digest, Sha1};

use super::license;

/// Largest file kept. Larger ones are nearly always data, logs or generated code.
pub const MAX_FILE_BYTES: u64 = 1 << 20;
/// Largest structured-data file (JSON, YAML, XML, ...) kept.
const MAX_DATA_BYTES: u64 = 128 << 10;
/// Code with longer lines is usually minified or embeds data.
const MAX_LINE: usize = 1000;
const MAX_MEAN_LINE: f64 = 100.0;
/// Below this share of alphanumeric characters (whitespace aside), a file is usually not prose or
/// code: ASCII art, separators, symbol dumps.
const MIN_ALNUM: f64 = 0.25;
/// How much of the start of a file is searched for license and generated-code markers.
const HEAD_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Code,
    /// Documentation: exempt from the line-length checks, since paragraphs are long lines.
    Prose,
    Data,
}

/// Directories of other people's code: package managers' trees and vendored copies.
const VENDORED_DIRS: &[&str] = &[
    "node_modules",
    "bower_components",
    "jspm_packages",
    "vendor",
    "vendors",
    "third_party",
    "third-party",
    "thirdparty",
    "3rdparty",
    "3rd_party",
    "3rd-party",
    "external",
    "extern",
    "deps",
    "pods",
    "carthage",
    "godeps",
    ".yarn",
    "venv",
    ".venv",
    "site-packages",
    "__pycache__",
];

/// Files written by tools rather than people.
const GENERATED_NAMES: &[&str] = &[
    "package-lock.json",
    "npm-shrinkwrap.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "cargo.lock",
    "composer.lock",
    "gemfile.lock",
    "poetry.lock",
    "pipfile.lock",
    "go.sum",
    "flake.lock",
    "packages.lock.json",
];
const GENERATED_SUFFIXES: &[&str] = &[
    ".min.js",
    ".min.css",
    "-min.js",
    ".bundle.js",
    ".pb.go",
    "_pb2.py",
    ".pb.cc",
    ".pb.h",
    ".g.dart",
    ".designer.cs",
];

/// The language of a path, from its file name or extension. None for anything else (images,
/// archives, fonts, unknown formats), which is not kept.
pub fn language(path: &str) -> Option<(&'static str, Kind)> {
    use Kind::*;
    let name = path.rsplit('/').next().unwrap_or(path);
    let by_name = match name {
        "Makefile" | "makefile" | "GNUmakefile" => Some(("Makefile", Code)),
        "Dockerfile" | "Containerfile" => Some(("Dockerfile", Code)),
        "CMakeLists.txt" => Some(("CMake", Code)),
        "BUILD" | "BUILD.bazel" | "WORKSPACE" | "MODULE.bazel" => Some(("Starlark", Code)),
        "meson.build" => Some(("Meson", Code)),
        "Rakefile" | "Gemfile" | "Podfile" | "Vagrantfile" => Some(("Ruby", Code)),
        "Jenkinsfile" => Some(("Groovy", Code)),
        "go.mod" => Some(("Go Module", Data)),
        _ => None,
    };
    if by_name.is_some() {
        return by_name;
    }
    // ".gitignore" and the like have no extension, only a leading dot.
    let (_, ext) = name.rsplit_once('.').filter(|(stem, _)| !stem.is_empty())?;
    Some(match ext.to_ascii_lowercase().as_str() {
        "rs" => ("Rust", Code),
        "py" | "pyi" => ("Python", Code),
        "pyx" | "pxd" => ("Cython", Code),
        "ipynb" => ("Jupyter Notebook", Code),
        "js" | "mjs" | "cjs" | "jsx" => ("JavaScript", Code),
        "ts" | "mts" | "cts" | "tsx" => ("TypeScript", Code),
        "go" => ("Go", Code),
        "java" => ("Java", Code),
        "kt" | "kts" => ("Kotlin", Code),
        "scala" | "sc" => ("Scala", Code),
        "groovy" | "gradle" => ("Groovy", Code),
        "clj" | "cljs" | "cljc" => ("Clojure", Code),
        "c" | "h" => ("C", Code),
        "cc" | "cpp" | "cxx" | "c++" | "hpp" | "hh" | "hxx" | "h++" | "ipp" | "tpp" | "inl" => {
            ("C++", Code)
        }
        "cu" | "cuh" => ("CUDA", Code),
        "m" => ("Objective-C", Code),
        "mm" => ("Objective-C++", Code),
        "swift" => ("Swift", Code),
        "cs" | "csx" => ("C#", Code),
        "fs" | "fsi" | "fsx" => ("F#", Code),
        "vb" => ("Visual Basic .NET", Code),
        "rb" | "rake" | "gemspec" => ("Ruby", Code),
        "php" => ("PHP", Code),
        "pl" | "pm" => ("Perl", Code),
        "lua" => ("Lua", Code),
        "r" => ("R", Code),
        "jl" => ("Julia", Code),
        "hs" | "lhs" => ("Haskell", Code),
        "ml" | "mli" => ("OCaml", Code),
        "ex" | "exs" => ("Elixir", Code),
        "erl" | "hrl" => ("Erlang", Code),
        "gleam" => ("Gleam", Code),
        "elm" => ("Elm", Code),
        "purs" => ("PureScript", Code),
        "dart" => ("Dart", Code),
        "zig" => ("Zig", Code),
        "nim" => ("Nim", Code),
        "cr" => ("Crystal", Code),
        "d" => ("D", Code),
        "f" | "for" | "f90" | "f95" | "f03" | "f08" => ("Fortran", Code),
        "pas" => ("Pascal", Code),
        "adb" | "ads" => ("Ada", Code),
        "cob" | "cbl" => ("COBOL", Code),
        "rkt" => ("Racket", Code),
        "scm" | "ss" => ("Scheme", Code),
        "lisp" => ("Common Lisp", Code),
        "el" => ("Emacs Lisp", Code),
        "vim" => ("Vim Script", Code),
        "tcl" => ("Tcl", Code),
        "hx" => ("Haxe", Code),
        "mojo" => ("Mojo", Code),
        "sol" => ("Solidity", Code),
        "sh" | "bash" | "zsh" => ("Shell", Code),
        "fish" => ("Fish", Code),
        "ps1" | "psm1" => ("PowerShell", Code),
        "bat" | "cmd" => ("Batchfile", Code),
        "sql" => ("SQL", Code),
        "html" | "htm" => ("HTML", Code),
        "css" => ("CSS", Code),
        "scss" => ("SCSS", Code),
        "sass" => ("Sass", Code),
        "less" => ("Less", Code),
        "vue" => ("Vue", Code),
        "svelte" => ("Svelte", Code),
        "astro" => ("Astro", Code),
        "s" | "asm" => ("Assembly", Code),
        "glsl" | "vert" | "frag" => ("GLSL", Code),
        "hlsl" => ("HLSL", Code),
        "wgsl" => ("WGSL", Code),
        "sv" | "svh" => ("SystemVerilog", Code),
        "vhd" | "vhdl" => ("VHDL", Code),
        "proto" => ("Protocol Buffer", Code),
        "thrift" => ("Thrift", Code),
        "graphql" | "gql" => ("GraphQL", Code),
        "tf" | "hcl" => ("HCL", Code),
        "nix" => ("Nix", Code),
        "bzl" | "star" => ("Starlark", Code),
        "cmake" => ("CMake", Code),
        "mk" => ("Makefile", Code),
        "dockerfile" => ("Dockerfile", Code),
        "jsonnet" | "libsonnet" => ("Jsonnet", Code),
        "tex" | "sty" => ("TeX", Code),
        "md" | "markdown" | "mdx" => ("Markdown", Prose),
        "rst" => ("reStructuredText", Prose),
        "adoc" | "asciidoc" => ("AsciiDoc", Prose),
        "org" => ("Org", Prose),
        "txt" => ("Text", Prose),
        "json" | "jsonc" | "json5" => ("JSON", Data),
        "yml" | "yaml" => ("YAML", Data),
        "toml" => ("TOML", Data),
        "xml" => ("XML", Data),
        "ini" => ("INI", Data),
        _ => return None,
    })
}

/// Why a path is not kept, judged from the path alone.
pub fn check_path(path: &str) -> Result<(&'static str, Kind), &'static str> {
    let dirs = || path.split('/').rev().skip(1);
    if dirs().any(|d| VENDORED_DIRS.iter().any(|v| v.eq_ignore_ascii_case(d))) {
        return Err("vendored");
    }
    // generated/, generated-src/, pregenerated/, __generated__/ ...
    if dirs().any(|d| d.to_ascii_lowercase().contains("generated")) {
        return Err("generated");
    }
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    if GENERATED_NAMES.contains(&name.as_str())
        || GENERATED_SUFFIXES.iter().any(|s| name.ends_with(s))
    {
        return Err("generated");
    }
    language(path).ok_or("unknown_type")
}

/// Why a file of `size` bytes is not kept, before reading it.
pub fn check_size(size: u64, kind: Kind) -> Result<(), &'static str> {
    let max = if kind == Kind::Data {
        MAX_DATA_BYTES
    } else {
        MAX_FILE_BYTES
    };
    match size {
        0 => Err("empty"),
        s if s > max => Err("too_big"),
        _ => Ok(()),
    }
}

/// The file as text, or why it is not kept, from its contents.
pub fn check_content<'a>(
    bytes: &'a [u8],
    kind: Kind,
    repo_license: &str,
) -> Result<&'a str, &'static str> {
    if bytes[..bytes.len().min(8192)].contains(&0) {
        return Err("binary");
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "not_utf8")?;
    if text.starts_with("version https://git-lfs.github.com/spec/") {
        return Err("lfs_pointer");
    }
    let head = &text[..floor_char_boundary(text, HEAD_BYTES)];
    if is_generated(head) {
        return Err("generated");
    }
    if !license::file_allowed(head, repo_license, kind == Kind::Code) {
        return Err("file_license");
    }
    if kind != Kind::Prose {
        let (mut lines, mut longest) = (0usize, 0usize);
        for line in text.lines() {
            lines += 1;
            longest = longest.max(line.len());
        }
        if longest > MAX_LINE || text.len() as f64 / lines.max(1) as f64 > MAX_MEAN_LINE {
            return Err("long_lines");
        }
    }
    let (mut alnum, mut visible) = (0usize, 0usize);
    for c in text.chars().filter(|c| !c.is_whitespace()) {
        visible += 1;
        alnum += c.is_alphanumeric() as usize;
    }
    if (alnum as f64) < MIN_ALNUM * visible as f64 {
        return Err("low_alnum");
    }
    Ok(text)
}

/// The markers code generators leave at the top of their output.
fn is_generated(head: &str) -> bool {
    let head = head.to_ascii_lowercase();
    head.contains("@generated")
        || (head.contains("generated") && head.contains("do not edit"))
        || head.contains("automatically generated")
        || head.contains("auto-generated")
        || head.contains("autogenerated")
}

fn floor_char_boundary(text: &str, at: usize) -> usize {
    let mut at = at.min(text.len());
    while !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// The file's git blob id: the sha1 git, GitHub and Software Heritage use, so exact duplicates
/// can be found across repositories and crawls.
pub fn blob_id(bytes: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn knows_languages() {
        assert_eq!(language("src/lib.rs"), Some(("Rust", Kind::Code)));
        assert_eq!(language("a/b/Makefile"), Some(("Makefile", Kind::Code)));
        assert_eq!(language("README.MD"), Some(("Markdown", Kind::Prose)));
        assert_eq!(language("config/app.yaml"), Some(("YAML", Kind::Data)));
        assert_eq!(language("logo.png"), None);
        assert_eq!(language(".gitignore"), None);
        assert_eq!(language("LICENSE"), None);
    }

    #[test]
    fn drops_paths_of_other_peoples_and_tools_code() {
        assert_eq!(check_path("vendor/github.com/x/y.go"), Err("vendored"));
        assert_eq!(
            check_path("web/node_modules/left-pad/index.js"),
            Err("vendored")
        );
        assert_eq!(
            check_path("src/Third_Party/zlib/inflate.c"),
            Err("vendored")
        );
        assert_eq!(check_path("static/jquery.min.js"), Err("generated"));
        assert_eq!(check_path("package-lock.json"), Err("generated"));
        assert_eq!(check_path("api/service.pb.go"), Err("generated"));
        assert_eq!(
            check_path("aws-lc/generated-src/err_data.c"),
            Err("generated")
        );
        assert_eq!(check_path("web/__generated__/types.ts"), Err("generated"));
        assert_eq!(check_path("assets/font.woff2"), Err("unknown_type"));
        assert_eq!(
            check_path("src/vendor.rs"),
            Ok(("Rust", Kind::Code)),
            "only directories count as vendored"
        );
    }

    #[test]
    fn checks_sizes() {
        assert_eq!(check_size(0, Kind::Code), Err("empty"));
        assert_eq!(check_size(500_000, Kind::Code), Ok(()));
        assert_eq!(check_size(500_000, Kind::Data), Err("too_big"));
        assert_eq!(check_size(MAX_FILE_BYTES + 1, Kind::Prose), Err("too_big"));
    }

    #[test]
    fn checks_contents() {
        let ok = |text: &str, kind| check_content(text.as_bytes(), kind, "MIT").map(|_| ());
        assert_eq!(
            ok("fn main() {\n    println!(\"hi\");\n}\n", Kind::Code),
            Ok(())
        );
        assert_eq!(
            check_content(b"\x89PNG\r\n\x1a\n\0\0", Kind::Code, "MIT"),
            Err("binary")
        );
        assert_eq!(
            check_content(b"caf\xe9", Kind::Code, "MIT"),
            Err("not_utf8")
        );
        assert_eq!(
            ok(
                "version https://git-lfs.github.com/spec/v1\noid sha256:ab\nsize 12\n",
                Kind::Data
            ),
            Err("lfs_pointer")
        );
        assert_eq!(
            ok(
                "// Code generated by protoc-gen-go. DO NOT EDIT.\npackage x\n",
                Kind::Code
            ),
            Err("generated")
        );
        assert_eq!(
            ok("// SPDX-License-Identifier: GPL-2.0\nint x;\n", Kind::Code),
            Err("file_license")
        );
        assert_eq!(
            ok(&format!("var x = \"{}\";\n", "a".repeat(2000)), Kind::Code),
            Err("long_lines")
        );
        assert!(
            ok(&format!("{}\n", "word ".repeat(400)), Kind::Prose).is_ok(),
            "prose may have long lines"
        );
        assert_eq!(
            ok("0x00, 0x01, 0x02, 0x03, 0x04, 0x05,\n", Kind::Code),
            Ok(())
        );
        assert_eq!(
            ok("--- ,,, ;;; ---\n=== ... ===\n", Kind::Code),
            Err("low_alnum")
        );
        // Indentation doesn't count against code (tokio's src/macros/loom.rs).
        let macros = "macro_rules! if_loom {\n    ($($t:tt)*) => {{\n        #[cfg(loom)]\n        {\n            $($t)*\n        }\n    }}\n}\n";
        assert_eq!(ok(macros, Kind::Code), Ok(()));
    }

    #[test]
    fn blob_ids_match_git() {
        // `printf 'hello\n' | git hash-object --stdin`
        assert_eq!(
            hex::encode(blob_id(b"hello\n")),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
    }
}
