use std::sync::Arc;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quickwit_config::{SourceInputFormat, TransformConfig};
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::actors::DocumentConverter;

const JSON_NORMAL: &str = include_str!("data/bench_data.json");
const JSON_LIGHT_TRANSFORM: &str = include_str!("data/bench_data_light_transform.json");
const JSON_HEAVY_TRANSFORM: &str = include_str!("data/bench_data_heavy_transform.json");

fn default_doc_mapper_for_bench() -> DocMapper {
    const JSON_CONFIG_VALUE: &str = r#"
        {
            "store_source": true,
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "tag_fields": ["id"],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "datetime",
                    "output_format": "unix_timestamp_secs",
                    "fast": true,
                    "input_formats": ["iso8601"]
                },
                { "name": "first_name", "type": "text", "stored": true },
                { "name": "last_name", "type": "text", "stored": true },
                { "name": "id", "type": "u64", "stored": true },
                { "name": "email", "type": "text", "stored": true },
                { "name": "job", "type": "text", "stored": true }
            ]
        }"#;
    serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap()
}

fn bench_converter(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    input: &str,
    transform: Option<TransformConfig>,
) {
    let lines: Vec<Bytes> = input
        .lines()
        .map(|line| Bytes::copy_from_slice(line.trim().as_bytes()))
        .collect();
    group.throughput(criterion::Throughput::Bytes(input.len() as u64));
    let doc_mapper = Arc::new(default_doc_mapper_for_bench());
    group.bench_function(BenchmarkId::new(name, "converter"), |b| {
        let mut converter = DocumentConverter::try_new(
            "bench-index".to_string(),
            "bench-source".to_string(),
            doc_mapper.clone(),
            transform.clone(),
            SourceInputFormat::Json,
        )
        .unwrap();
        b.iter(|| {
            let mut processed = Vec::with_capacity(lines.len());
            for doc in &lines {
                converter.process_raw_doc(doc.clone(), &mut processed);
            }
            processed
        })
    });
}

fn bench_doc_process(c: &mut Criterion) {
    let mut group = c.benchmark_group("doc_process");
    bench_converter(&mut group, "no_transform", JSON_NORMAL, None);
    bench_converter(
        &mut group,
        "light_transform",
        JSON_LIGHT_TRANSFORM,
        Some(TransformConfig::for_test(
            ".first_name = upcase!(string!(.first_name))",
        )),
    );
    bench_converter(
        &mut group,
        "heavy_transform",
        JSON_HEAVY_TRANSFORM,
        Some(TransformConfig::for_test(
            ".email = downcase!(string!(.email))",
        )),
    );
    group.finish();
}

criterion_group!(benches, bench_doc_process);
criterion_main!(benches);
