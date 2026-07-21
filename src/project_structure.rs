//! Native, rebuildable structural projection for current project source.
//!
//! Tree-sitter output is navigation metadata only. Exact working-tree bytes,
//! re-verified by the project index, remain the evidence boundary.

use std::{collections::BTreeSet, path::Path, time::Instant};

use serde::Serialize;
use tree_sitter::{Language, Parser, Query, QueryCursor, StreamingIterator};

pub const STRUCTURAL_POLICY_VERSION: &str =
    "tree_sitter_structural_v2_qualified_calls_v1_identifier_split_v1_adaptive_timeout_v1";
pub const STRUCTURAL_GRAMMAR_REVISION: &str = concat!(
    "tree-sitter=0.25.10;",
    "rust=0.24.2;python=0.25.0;javascript=0.25.0;",
    "typescript=0.23.2;go=0.25.0"
);
const PARSE_BASE_TIMEOUT_MICROS: u64 = 100_000;
const PARSE_MAX_TIMEOUT_MICROS: u64 = 750_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralState {
    Ready,
    PartialParse,
    Unsupported,
    ParseError,
    TimedOut,
}

impl StructuralState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::PartialParse => "partial_parse",
            Self::Unsupported => "unsupported",
            Self::ParseError => "parse_error",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralLanguage {
    Rust,
    Python,
    JavaScript,
    TypeScript,
    Tsx,
    Go,
}

impl StructuralLanguage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
            Self::JavaScript => "javascript",
            Self::TypeScript => "typescript",
            Self::Tsx => "tsx",
            Self::Go => "go",
        }
    }

    fn grammar(self) -> Language {
        match self {
            Self::Rust => tree_sitter_rust::LANGUAGE.into(),
            Self::Python => tree_sitter_python::LANGUAGE.into(),
            Self::JavaScript => tree_sitter_javascript::LANGUAGE.into(),
            Self::TypeScript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            Self::Tsx => tree_sitter_typescript::LANGUAGE_TSX.into(),
            Self::Go => tree_sitter_go::LANGUAGE.into(),
        }
    }

    fn tags_query(self) -> String {
        match self {
            Self::Rust => tree_sitter_rust::TAGS_QUERY.to_owned(),
            Self::Python => tree_sitter_python::TAGS_QUERY.to_owned(),
            Self::JavaScript => tree_sitter_javascript::TAGS_QUERY.to_owned(),
            Self::TypeScript | Self::Tsx => format!(
                "{}\n{}",
                tree_sitter_javascript::TAGS_QUERY,
                tree_sitter_typescript::TAGS_QUERY
            ),
            Self::Go => tree_sitter_go::TAGS_QUERY.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralEdgeKind {
    Contains,
    Calls,
    Imports,
    Inherits,
    TestedBy,
}

impl StructuralEdgeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Contains => "contains",
            Self::Calls => "calls",
            Self::Imports => "imports",
            Self::Inherits => "inherits",
            Self::TestedBy => "tested_by",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralConfidence {
    Extracted,
    Inferred,
    Ambiguous,
}

impl StructuralConfidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Extracted => "extracted",
            Self::Inferred => "inferred",
            Self::Ambiguous => "ambiguous",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedSymbol {
    pub symbol_key: String,
    pub name: String,
    pub kind: String,
    pub qualified_name: String,
    pub parent_key: Option<String>,
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub is_test: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedReference {
    pub source_key: Option<String>,
    pub target_name: String,
    pub kind: StructuralEdgeKind,
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone)]
pub struct ParsedFileStructure {
    pub language: Option<StructuralLanguage>,
    pub state: StructuralState,
    pub symbols: Vec<ParsedSymbol>,
    pub references: Vec<ParsedReference>,
    pub parse_ms: f64,
}

#[derive(Debug, Clone)]
struct RawSymbol {
    name: String,
    kind: String,
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    end_line: usize,
}

#[derive(Debug, Clone)]
struct RawReference {
    target_name: String,
    kind: StructuralEdgeKind,
    start_byte: usize,
    end_byte: usize,
    source_name: Option<String>,
}

pub fn detect_language(path: &str) -> Option<StructuralLanguage> {
    let path = Path::new(path);
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "rs" => Some(StructuralLanguage::Rust),
        "py" | "pyi" => Some(StructuralLanguage::Python),
        "js" | "jsx" | "mjs" | "cjs" => Some(StructuralLanguage::JavaScript),
        "ts" | "mts" | "cts" => Some(StructuralLanguage::TypeScript),
        "tsx" => Some(StructuralLanguage::Tsx),
        "go" => Some(StructuralLanguage::Go),
        _ => None,
    }
}

pub fn parse_file(path: &str, text: &str) -> ParsedFileStructure {
    let started = Instant::now();
    let Some(language_kind) = detect_language(path) else {
        return ParsedFileStructure {
            language: None,
            state: StructuralState::Unsupported,
            symbols: vec![],
            references: vec![],
            parse_ms: started.elapsed().as_secs_f64() * 1_000.0,
        };
    };
    let language = language_kind.grammar();
    let mut parser = Parser::new();
    if parser.set_language(&language).is_err() {
        return failed_parse(language_kind, StructuralState::ParseError, started);
    }
    #[allow(deprecated)]
    parser.set_timeout_micros(parser_timeout_micros(text.len()));
    let Some(tree) = parser.parse(text.as_bytes(), None) else {
        return failed_parse(language_kind, StructuralState::TimedOut, started);
    };
    let recovered_with_errors = tree.root_node().has_error();
    let mut error_spans = Vec::new();
    collect_error_spans(tree.root_node(), &mut error_spans);
    let query_source = language_kind.tags_query();
    let Ok(query) = Query::new(&language, &query_source) else {
        return failed_parse(language_kind, StructuralState::ParseError, started);
    };
    let capture_names = query.capture_names();
    let mut raw_symbols = Vec::new();
    let mut raw_references = Vec::new();
    let mut query_cursor = QueryCursor::new();
    let mut matches = query_cursor.matches(&query, tree.root_node(), text.as_bytes());
    while let Some(query_match) = matches.next() {
        let mut name = None;
        let mut definition = None;
        let mut reference = None;
        for capture in query_match.captures {
            let capture_name = capture_names[capture.index as usize];
            if capture_name == "name" {
                name = node_text(capture.node, text).map(str::to_owned);
            } else if let Some(kind) = capture_name.strip_prefix("definition.") {
                definition = Some((kind.to_owned(), capture.node));
            } else if capture_name == "reference.call" {
                reference = Some((StructuralEdgeKind::Calls, capture.node));
            }
        }
        let Some(name) = name.and_then(|name| normalized_name(&name)) else {
            continue;
        };
        if let Some((kind, node)) = definition {
            raw_symbols.push(RawSymbol {
                name,
                kind,
                start_byte: node.start_byte(),
                end_byte: node.end_byte(),
                start_line: node.start_position().row + 1,
                end_line: node.end_position().row + 1,
            });
        } else if let Some((kind, node)) = reference {
            raw_references.push(RawReference {
                target_name: name,
                kind,
                start_byte: node.start_byte(),
                end_byte: node.end_byte(),
                source_name: None,
            });
        }
    }
    collect_additional_definitions(tree.root_node(), text, language_kind, &mut raw_symbols);
    collect_structural_relationships(tree.root_node(), text, language_kind, &mut raw_references);
    if recovered_with_errors {
        raw_symbols
            .retain(|symbol| !overlaps_error(symbol.start_byte, symbol.end_byte, &error_spans));
        raw_references.retain(|reference| {
            !overlaps_error(reference.start_byte, reference.end_byte, &error_spans)
        });
        if raw_symbols.is_empty() {
            return failed_parse(language_kind, StructuralState::ParseError, started);
        }
    }
    let mut symbols = finalize_symbols(path, text, raw_symbols);
    let file_symbol = ParsedSymbol {
        symbol_key: symbol_key(path, "file", path, 0, text.len()),
        name: Path::new(path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(path)
            .to_owned(),
        kind: "file".into(),
        qualified_name: path.to_owned(),
        parent_key: None,
        start_byte: 0,
        end_byte: text.len(),
        start_line: 1,
        end_line: text.bytes().filter(|byte| *byte == b'\n').count() + 1,
        is_test: test_shaped_path(path),
    };
    let file_key = file_symbol.symbol_key.clone();
    symbols.push(file_symbol);
    symbols.sort_by(|left, right| {
        left.start_byte
            .cmp(&right.start_byte)
            .then_with(|| right.end_byte.cmp(&left.end_byte))
            .then_with(|| left.qualified_name.cmp(&right.qualified_name))
    });
    let mut references = Vec::new();
    let mut seen = BTreeSet::new();
    for reference in raw_references {
        let named_sources = reference
            .source_name
            .as_ref()
            .map(|name| {
                symbols
                    .iter()
                    .filter(|symbol| symbol.kind != "file" && symbol.name == *name)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let source_key = if named_sources.len() == 1 {
            Some(named_sources[0].symbol_key.clone())
        } else {
            innermost_symbol(&symbols, reference.start_byte, reference.end_byte)
                .map(|symbol| symbol.symbol_key.clone())
                .or_else(|| Some(file_key.clone()))
        };
        let dedupe = (
            source_key.clone(),
            reference.source_name.clone(),
            reference.target_name.clone(),
            reference.kind.as_str(),
            reference.start_byte,
        );
        if seen.insert(dedupe) {
            references.push(ParsedReference {
                source_key,
                target_name: reference.target_name,
                kind: reference.kind,
                start_byte: reference.start_byte,
                end_byte: reference.end_byte,
            });
        }
    }
    ParsedFileStructure {
        language: Some(language_kind),
        state: if recovered_with_errors {
            StructuralState::PartialParse
        } else {
            StructuralState::Ready
        },
        symbols,
        references,
        parse_ms: started.elapsed().as_secs_f64() * 1_000.0,
    }
}

fn parser_timeout_micros(byte_count: usize) -> u64 {
    PARSE_BASE_TIMEOUT_MICROS
        .saturating_add(byte_count as u64)
        .min(PARSE_MAX_TIMEOUT_MICROS)
}

fn collect_error_spans(node: tree_sitter::Node<'_>, spans: &mut Vec<(usize, usize)>) {
    if node.is_error() || node.is_missing() {
        spans.push((
            node.start_byte(),
            node.end_byte().max(node.start_byte() + 1),
        ));
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.has_error() || child.is_error() || child.is_missing() {
            collect_error_spans(child, spans);
        }
    }
}

fn overlaps_error(start: usize, end: usize, errors: &[(usize, usize)]) -> bool {
    errors
        .iter()
        .any(|(error_start, error_end)| start < *error_end && end > *error_start)
}

fn failed_parse(
    language: StructuralLanguage,
    state: StructuralState,
    started: Instant,
) -> ParsedFileStructure {
    ParsedFileStructure {
        language: Some(language),
        state,
        symbols: vec![],
        references: vec![],
        parse_ms: started.elapsed().as_secs_f64() * 1_000.0,
    }
}

fn collect_additional_definitions(
    node: tree_sitter::Node<'_>,
    text: &str,
    language: StructuralLanguage,
    symbols: &mut Vec<RawSymbol>,
) {
    let kind = match (language, node.kind()) {
        (StructuralLanguage::Rust, "function_signature_item") => Some("method"),
        (StructuralLanguage::TypeScript | StructuralLanguage::Tsx, "method_signature") => {
            Some("method")
        }
        (StructuralLanguage::Go, "method_elem") => Some("method"),
        _ => None,
    };
    if let Some(kind) = kind
        && let Some(name_node) = node.child_by_field_name("name")
        && let Some(name) = node_text(name_node, text).and_then(normalized_name)
    {
        symbols.push(RawSymbol {
            name,
            kind: kind.into(),
            start_byte: node.start_byte(),
            end_byte: node.end_byte(),
            start_line: node.start_position().row + 1,
            end_line: node.end_position().row + 1,
        });
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_additional_definitions(child, text, language, symbols);
    }
}

fn finalize_symbols(path: &str, text: &str, mut raw: Vec<RawSymbol>) -> Vec<ParsedSymbol> {
    raw.sort_by(|left, right| {
        left.start_byte
            .cmp(&right.start_byte)
            .then_with(|| right.end_byte.cmp(&left.end_byte))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    raw.dedup_by(|left, right| {
        left.start_byte == right.start_byte
            && left.end_byte == right.end_byte
            && left.name == right.name
    });
    let mut symbols = Vec::new();
    for (index, symbol) in raw.iter().enumerate() {
        let mut parents = raw
            .iter()
            .enumerate()
            .filter(|(candidate_index, candidate)| {
                *candidate_index != index
                    && candidate.start_byte <= symbol.start_byte
                    && candidate.end_byte >= symbol.end_byte
                    && (candidate.start_byte < symbol.start_byte
                        || candidate.end_byte > symbol.end_byte)
            })
            .collect::<Vec<_>>();
        parents.sort_by_key(|(_, parent)| parent.end_byte - parent.start_byte);
        let parent = parents.first().map(|(_, parent)| *parent);
        let mut lineage = raw
            .iter()
            .filter(|candidate| {
                candidate.start_byte <= symbol.start_byte
                    && candidate.end_byte >= symbol.end_byte
                    && (candidate.start_byte < symbol.start_byte
                        || candidate.end_byte > symbol.end_byte)
            })
            .collect::<Vec<_>>();
        lineage
            .sort_by_key(|candidate| std::cmp::Reverse(candidate.end_byte - candidate.start_byte));
        let mut names = lineage
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>();
        names.push(&symbol.name);
        let qualified_name = format!("{path}::{}", names.join("."));
        let parent_key = parent.map(|parent| {
            let mut parent_lineage = raw
                .iter()
                .filter(|candidate| {
                    candidate.start_byte <= parent.start_byte
                        && candidate.end_byte >= parent.end_byte
                        && (candidate.start_byte < parent.start_byte
                            || candidate.end_byte > parent.end_byte)
                })
                .collect::<Vec<_>>();
            parent_lineage.sort_by_key(|candidate| {
                std::cmp::Reverse(candidate.end_byte - candidate.start_byte)
            });
            let mut parent_names = parent_lineage
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>();
            parent_names.push(&parent.name);
            let parent_qualified = format!("{path}::{}", parent_names.join("."));
            symbol_key(
                path,
                &parent.kind,
                &parent_qualified,
                parent.start_byte,
                parent.end_byte,
            )
        });
        let mut prefix_start = symbol.start_byte.saturating_sub(160);
        while prefix_start < symbol.start_byte && !text.is_char_boundary(prefix_start) {
            prefix_start += 1;
        }
        let prefix = &text[prefix_start..symbol.start_byte];
        let is_test = test_shaped_path(path)
            || symbol.name.starts_with("test_")
            || symbol.name.ends_with("_test")
            || prefix.contains("#[test]")
            || prefix.contains("::test]");
        symbols.push(ParsedSymbol {
            symbol_key: symbol_key(
                path,
                &symbol.kind,
                &qualified_name,
                symbol.start_byte,
                symbol.end_byte,
            ),
            name: symbol.name.clone(),
            kind: symbol.kind.clone(),
            qualified_name,
            parent_key,
            start_byte: symbol.start_byte,
            end_byte: symbol.end_byte,
            start_line: symbol.start_line,
            end_line: symbol.end_line,
            is_test,
        });
    }
    symbols
}

fn symbol_key(path: &str, kind: &str, qualified: &str, start: usize, end: usize) -> String {
    let material = format!("{path}\0{kind}\0{qualified}\0{start}\0{end}");
    format!("sym_{}", &blake3::hash(material.as_bytes()).to_hex()[..32])
}

fn innermost_symbol(symbols: &[ParsedSymbol], start: usize, end: usize) -> Option<&ParsedSymbol> {
    symbols
        .iter()
        .filter(|symbol| {
            symbol.kind != "file" && symbol.start_byte <= start && symbol.end_byte >= end
        })
        .min_by_key(|symbol| symbol.end_byte - symbol.start_byte)
}

fn collect_structural_relationships(
    node: tree_sitter::Node<'_>,
    text: &str,
    language: StructuralLanguage,
    references: &mut Vec<RawReference>,
) {
    let kind = node.kind();
    if is_call_node(language, kind)
        && let Some(function) = node.child_by_field_name("function")
        && let Some(target_name) = call_target_name(function, text)
    {
        references.push(RawReference {
            target_name,
            kind: StructuralEdgeKind::Calls,
            start_byte: node.start_byte(),
            end_byte: node.end_byte(),
            source_name: None,
        });
    }
    let is_import = match language {
        StructuralLanguage::Rust => kind == "use_declaration",
        StructuralLanguage::Python => {
            matches!(kind, "import_statement" | "import_from_statement")
        }
        StructuralLanguage::JavaScript
        | StructuralLanguage::TypeScript
        | StructuralLanguage::Tsx => kind == "import_statement",
        StructuralLanguage::Go => kind == "import_spec",
    };
    if is_import && let Some(raw) = node_text(node, text) {
        for target in import_targets(language, raw) {
            references.push(RawReference {
                target_name: target,
                kind: StructuralEdgeKind::Imports,
                start_byte: node.start_byte(),
                end_byte: node.end_byte(),
                source_name: None,
            });
        }
    }
    collect_inheritance(node, text, language, references);
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_structural_relationships(child, text, language, references);
    }
}

fn is_call_node(language: StructuralLanguage, kind: &str) -> bool {
    match language {
        StructuralLanguage::Python => kind == "call",
        StructuralLanguage::Rust
        | StructuralLanguage::JavaScript
        | StructuralLanguage::TypeScript
        | StructuralLanguage::Tsx
        | StructuralLanguage::Go => kind == "call_expression",
    }
}

fn call_target_name(node: tree_sitter::Node<'_>, text: &str) -> Option<String> {
    if matches!(
        node.kind(),
        "identifier"
            | "field_identifier"
            | "property_identifier"
            | "type_identifier"
            | "shorthand_property_identifier"
    ) {
        return node_text(node, text).and_then(normalized_name);
    }
    for field in ["field", "attribute", "property", "name", "function"] {
        if let Some(child) = node.child_by_field_name(field)
            && let Some(name) = call_target_name(child, text)
        {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    let children = node.named_children(&mut cursor).collect::<Vec<_>>();
    for child in children.into_iter().rev() {
        if matches!(
            child.kind(),
            "type_arguments" | "type_parameters" | "arguments"
        ) {
            continue;
        }
        if let Some(name) = call_target_name(child, text) {
            return Some(name);
        }
    }
    node_text(node, text).and_then(last_identifier)
}

fn collect_inheritance(
    node: tree_sitter::Node<'_>,
    text: &str,
    language: StructuralLanguage,
    references: &mut Vec<RawReference>,
) {
    match language {
        StructuralLanguage::Python if node.kind() == "class_definition" => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|node| node_text(node, text))
            else {
                return;
            };
            if let Some(superclasses) = node.child_by_field_name("superclasses") {
                for target in identifier_texts(superclasses, text) {
                    references.push(RawReference {
                        target_name: target,
                        kind: StructuralEdgeKind::Inherits,
                        start_byte: superclasses.start_byte(),
                        end_byte: superclasses.end_byte(),
                        source_name: normalized_name(name),
                    });
                }
            }
        }
        StructuralLanguage::JavaScript
        | StructuralLanguage::TypeScript
        | StructuralLanguage::Tsx
            if matches!(
                node.kind(),
                "class_declaration" | "abstract_class_declaration" | "interface_declaration"
            ) =>
        {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|node| node_text(node, text))
            else {
                return;
            };
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if matches!(
                    child.kind(),
                    "class_heritage" | "extends_type_clause" | "implements_clause"
                ) {
                    for target in identifier_texts(child, text) {
                        references.push(RawReference {
                            target_name: target,
                            kind: StructuralEdgeKind::Inherits,
                            start_byte: child.start_byte(),
                            end_byte: child.end_byte(),
                            source_name: normalized_name(name),
                        });
                    }
                }
            }
        }
        StructuralLanguage::Rust if node.kind() == "impl_item" => {
            let Some(implementation) = node_text(node, text) else {
                return;
            };
            let header = implementation.split('{').next().unwrap_or(implementation);
            let Some((trait_text, type_text)) =
                header.trim_start_matches("impl").trim().split_once(" for ")
            else {
                return;
            };
            if let (Some(source), Some(target)) =
                (last_identifier(type_text), last_identifier(trait_text))
            {
                references.push(RawReference {
                    source_name: Some(source),
                    target_name: target,
                    kind: StructuralEdgeKind::Inherits,
                    start_byte: node.start_byte(),
                    end_byte: node.end_byte(),
                });
            }
        }
        _ => {}
    }
}

fn identifier_texts(node: tree_sitter::Node<'_>, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    collect_identifier_texts(node, text, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_identifier_texts(node: tree_sitter::Node<'_>, text: &str, out: &mut Vec<String>) {
    if matches!(
        node.kind(),
        "identifier" | "type_identifier" | "property_identifier"
    ) && let Some(name) = node_text(node, text).and_then(normalized_name)
    {
        if !matches!(name.as_str(), "extends" | "implements") {
            out.push(name);
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_identifier_texts(child, text, out);
    }
}

fn import_targets(language: StructuralLanguage, raw: &str) -> Vec<String> {
    let mut targets = BTreeSet::new();
    match language {
        StructuralLanguage::JavaScript
        | StructuralLanguage::TypeScript
        | StructuralLanguage::Tsx
        | StructuralLanguage::Go => {
            for quoted in quoted_segments(raw) {
                if let Some(target) = module_tail(&quoted) {
                    targets.insert(target);
                }
            }
        }
        StructuralLanguage::Python => {
            let head = raw
                .trim_start_matches("from ")
                .split_whitespace()
                .next()
                .unwrap_or_default();
            if let Some(target) = module_tail(head) {
                targets.insert(target);
            }
        }
        StructuralLanguage::Rust => {
            let head = raw
                .trim_start_matches("pub ")
                .trim_start_matches("use ")
                .split(['{', ';'])
                .next()
                .unwrap_or_default();
            for segment in head.split("::") {
                if let Some(target) = normalized_name(segment)
                    && !matches!(target.as_str(), "crate" | "self" | "super")
                {
                    targets.insert(target);
                }
            }
        }
    }
    targets.into_iter().collect()
}

fn quoted_segments(raw: &str) -> Vec<String> {
    let mut segments = Vec::new();
    for quote in ['\'', '"', '`'] {
        let mut remaining = raw;
        while let Some((_, tail)) = remaining.split_once(quote) {
            let Some((value, after)) = tail.split_once(quote) else {
                break;
            };
            if !value.is_empty() {
                segments.push(value.to_owned());
            }
            remaining = after;
        }
    }
    segments
}

fn module_tail(value: &str) -> Option<String> {
    value
        .trim_matches(['.', '/', '\'', '"', '`', ';'])
        .split(['/', '.'])
        .filter_map(normalized_name)
        .next_back()
}

fn last_identifier(value: &str) -> Option<String> {
    value
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter_map(normalized_name)
        .next_back()
}

fn normalized_name(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= 256).then(|| value.to_owned())
}

fn node_text<'a>(node: tree_sitter::Node<'_>, text: &'a str) -> Option<&'a str> {
    text.get(node.start_byte()..node.end_byte())
}

fn test_shaped_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("tests/")
        || lower.contains("/tests/")
        || lower.contains("/__tests__/")
        || lower.ends_with("_test.go")
        || lower.ends_with(".test.ts")
        || lower.ends_with(".test.tsx")
        || lower.ends_with(".spec.ts")
        || lower.ends_with(".spec.tsx")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_rust_symbols_calls_and_tests() {
        let parsed = parse_file(
            "src/lib.rs",
            "fn helper() {}\nfn run() { helper(); }\n#[test]\nfn run_test() { run(); }\n",
        );
        assert_eq!(parsed.state, StructuralState::Ready);
        assert!(parsed.symbols.iter().any(|symbol| symbol.name == "helper"));
        assert!(parsed.symbols.iter().any(|symbol| symbol.name == "run"));
        assert!(
            parsed
                .symbols
                .iter()
                .any(|symbol| symbol.name == "run_test" && symbol.is_test)
        );
        assert!(
            parsed
                .references
                .iter()
                .any(|reference| reference.target_name == "helper")
        );
    }

    #[test]
    fn extracts_qualified_and_generic_call_targets() {
        let rust = parse_file(
            "src/bin/tool.rs",
            r#"
fn main() {
    crate::eval::run_retrieval_eval_with_options(Default::default());
    service.fetch::<ResultType>();
}
"#,
        );
        let rust_targets = rust
            .references
            .iter()
            .filter(|reference| reference.kind == StructuralEdgeKind::Calls)
            .map(|reference| reference.target_name.as_str())
            .collect::<BTreeSet<_>>();
        assert!(rust_targets.contains("run_retrieval_eval_with_options"));
        assert!(rust_targets.contains("fetch"));
        assert!(!rust_targets.contains("ResultType"));
        let qualified = rust
            .references
            .iter()
            .find(|reference| reference.target_name == "run_retrieval_eval_with_options")
            .unwrap();
        assert_eq!(
            &r#"
fn main() {
    crate::eval::run_retrieval_eval_with_options(Default::default());
    service.fetch::<ResultType>();
}
"#[qualified.start_byte..qualified.end_byte],
            "crate::eval::run_retrieval_eval_with_options(Default::default())"
        );

        let python = parse_file(
            "app/service.py",
            "def run():\n    package.worker.execute()\n",
        );
        assert!(python.references.iter().any(|reference| {
            reference.kind == StructuralEdgeKind::Calls && reference.target_name == "execute"
        }));

        let typescript = parse_file(
            "src/client.ts",
            "export function run() { client.transport.send(); }\n",
        );
        assert!(typescript.references.iter().any(|reference| {
            reference.kind == StructuralEdgeKind::Calls && reference.target_name == "send"
        }));

        let go = parse_file(
            "cmd/tool/main.go",
            "package main\nfunc main() { service.Execute() }\n",
        );
        assert!(go.references.iter().any(|reference| {
            reference.kind == StructuralEdgeKind::Calls && reference.target_name == "Execute"
        }));
    }

    #[test]
    fn extracts_python_types_methods_calls_and_inheritance() {
        let parsed = parse_file(
            "app/service.py",
            "class Base:\n    pass\nclass Service(Base):\n    def run(self):\n        helper()\n",
        );
        assert_eq!(parsed.state, StructuralState::Ready);
        assert!(parsed.symbols.iter().any(|symbol| symbol.name == "Service"));
        assert!(
            parsed
                .symbols
                .iter()
                .any(|symbol| symbol.qualified_name.ends_with("Service.run"))
        );
        assert!(parsed.references.iter().any(|reference| {
            reference.kind == StructuralEdgeKind::Inherits && reference.target_name == "Base"
        }));
    }

    #[test]
    fn extracts_typescript_and_go_symbols() {
        let typescript = parse_file(
            "src/service.ts",
            "class Base {}\nclass Service extends Base { run() { helper(); } }\n",
        );
        assert_eq!(typescript.state, StructuralState::Ready);
        assert!(
            typescript
                .symbols
                .iter()
                .any(|symbol| symbol.name == "Service")
        );
        let go = parse_file(
            "service.go",
            "package service\ntype Service struct{}\nfunc (s Service) Run() { helper() }\n",
        );
        assert_eq!(go.state, StructuralState::Ready);
        assert!(go.symbols.iter().any(|symbol| symbol.name == "Run"));
        assert!(
            go.references
                .iter()
                .any(|reference| reference.target_name == "helper")
        );
    }

    #[test]
    fn unsupported_and_invalid_files_degrade_without_symbols() {
        let unsupported = parse_file("README.md", "# title");
        assert_eq!(unsupported.state, StructuralState::Unsupported);
        assert!(unsupported.symbols.is_empty());
        let invalid = parse_file("broken.py", "def broken(:\n");
        assert_eq!(invalid.state, StructuralState::ParseError);
        assert!(invalid.symbols.is_empty());
    }

    #[test]
    fn partial_parse_keeps_only_valid_non_error_symbols() {
        let partial = parse_file(
            "src/partial.rs",
            "pub fn valid_before() {}\nfn broken(\npub fn valid_after() {}\n",
        );
        assert_eq!(partial.state, StructuralState::PartialParse);
        assert!(
            partial
                .symbols
                .iter()
                .any(|symbol| symbol.name == "valid_before")
        );
        assert!(partial.symbols.iter().all(|symbol| symbol.name != "broken"));
    }

    #[test]
    fn parser_timeout_is_size_adaptive_and_hard_bounded() {
        assert_eq!(parser_timeout_micros(0), 100_000);
        assert_eq!(parser_timeout_micros(100_000), 200_000);
        assert_eq!(parser_timeout_micros(usize::MAX), 750_000);
    }

    #[test]
    fn current_project_index_methods_remain_structurally_visible() {
        let parsed = parse_file("src/project_index.rs", include_str!("project_index.rs"));
        assert!(
            matches!(
                parsed.state,
                StructuralState::Ready | StructuralState::PartialParse
            ),
            "state was {:?}",
            parsed.state
        );
        assert!(
            parsed.symbols.iter().any(|symbol| symbol.name == "map"),
            "map method missing; nearby names={:?}",
            parsed
                .symbols
                .iter()
                .filter(|symbol| symbol.qualified_name.contains("ProjectIndex"))
                .map(|symbol| symbol.name.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn labeled_definition_corpus_meets_recall_and_exact_span_gate() {
        let fixtures = [
            (
                "src/lib.rs",
                "struct Engine;\nenum Mode { Fast }\ntrait Runner { fn run(&self); }\nfn helper() {}\nimpl Engine { fn start(&self) { helper(); } }\n",
                &["Engine", "Mode", "Runner", "run", "helper", "start"][..],
            ),
            (
                "app/service.py",
                "class Base:\n    def stop(self):\n        pass\nclass Worker(Base):\n    def run(self):\n        helper()\ndef helper():\n    pass\n",
                &["Base", "stop", "Worker", "run", "helper"][..],
            ),
            (
                "src/service.js",
                "class Base { stop() {} }\nclass Service extends Base { run() { helper(); } }\nfunction helper() {}\n",
                &["Base", "stop", "Service", "run", "helper"][..],
            ),
            (
                "src/service.ts",
                "interface Store { get(): string }\nclass Service { run() { helper(); } }\nfunction helper(): void {}\n",
                &["Store", "get", "Service", "run", "helper"][..],
            ),
            (
                "src/panel.tsx",
                "function Panel() { return <div />; }\nclass Boundary { render() { return <Panel />; } }\n",
                &["Panel", "Boundary", "render"][..],
            ),
            (
                "service.go",
                "package service\ntype Service struct{}\ntype Runner interface { Run() }\nfunc helper() {}\nfunc (Service) Run() { helper() }\n",
                &["Service", "Runner", "Run", "helper"][..],
            ),
        ];
        for (path, source, expected) in fixtures {
            let parsed = parse_file(path, source);
            assert_eq!(parsed.state, StructuralState::Ready, "{path}");
            let names = parsed
                .symbols
                .iter()
                .filter(|symbol| symbol.kind != "file")
                .map(|symbol| symbol.name.as_str())
                .collect::<BTreeSet<_>>();
            let found = expected
                .iter()
                .filter(|name| names.contains(**name))
                .count();
            let recall = found as f64 / expected.len() as f64;
            assert!(
                recall >= 0.95,
                "definition recall for {path} was {recall:.3}; names={names:?}"
            );
            for symbol in parsed.symbols.iter().filter(|symbol| symbol.kind != "file") {
                assert!(source.is_char_boundary(symbol.start_byte), "{path}");
                assert!(source.is_char_boundary(symbol.end_byte), "{path}");
                let exact = &source[symbol.start_byte..symbol.end_byte];
                assert!(
                    exact.contains(&symbol.name),
                    "{path}: exact span for {} did not contain its name",
                    symbol.qualified_name
                );
            }
        }
    }
}
