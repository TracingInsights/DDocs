from pathlib import Path

from extract import (
    ALL_DOC_TYPES,
    PdfJob,
    detect_doc_type,
    humanize_title,
    is_header_image,
    is_low_value_native_image,
    is_tiny_non_content_image,
    merge_image_bboxes,
    merge_table_continuations,
    needs_extraction,
    normalize_date,
    normalize_pdf_table,
    parse_decision_fields,
    parse_header_fields,
    should_render_pages,
    table_to_markdown,
)


def test_detect_doc_type_handles_current_and_historical_names() -> None:
    assert detect_doc_type(Path("decision-car-22-alleged-unsafe-release.pdf")) == "decision"
    assert detect_doc_type(Path("2018_spanish_grand_prix_2018_offence_-_car_2.pdf")) == "offence"
    parc_ferme_pdf = Path("parts-and-parameters-been-replaced-and-or-changed-during-parc-fermé.pdf")
    assert detect_doc_type(parc_ferme_pdf) == "parc-ferme"
    assert detect_doc_type(Path("race-directors-competition-notes.pdf")) == "race-director-note"
    assert detect_doc_type(Path("event-notes-circuit-map-pit-lane-and-quarantine-zone.pdf")) == "circuit-map"
    assert detect_doc_type(Path("post-race-checks-on-car-numbers-81-and-16-2025-miami-gp.pdf")) == "post-race-checks"
    assert detect_doc_type(Path("event-notes-pirelli-preview-v2.pdf")) == "pirelli-preview"
    assert detect_doc_type(Path("post-qualifying-procedure.pdf")) == "procedure"
    assert detect_doc_type(Path("failed-rear-wing-main-plane-deflection-test-on-car-31.pdf")) == "technical-check"
    assert detect_doc_type(Path("totally-unmatched.pdf")) == "other"


def test_detect_doc_type_covers_2025_reference_corpus() -> None:
    pdfs = list(Path("documents/2025").rglob("*.pdf"))
    if not pdfs:
        return
    unmatched = [path.as_posix() for path in pdfs if detect_doc_type(path) == "other"]
    unknown_types = {detect_doc_type(path) for path in pdfs} - ALL_DOC_TYPES
    assert unknown_types == set()
    assert unmatched == []


def test_parse_header_fields_extracts_common_fia_header() -> None:
    text = """
    FIA FORMULA 1 WORLD CHAMPIONSHIP
    2024 AUSTRIAN GRAND PRIX
    28 June - 30 June 2024
    From The Stewards Document 65
    To The Team Manager, Red Bull Racing Date 30 June 2024
    Time 16:22
    """

    assert parse_header_fields(text) == {
        "event_name": "Austrian Grand Prix",
        "doc_number": "65",
        "date": "2024-06-30",
        "time": "16:22",
        "from": "The Stewards",
        "to": "The Team Manager, Red Bull Racing",
    }


def test_parse_decision_fields_extracts_p0_metadata() -> None:
    text = """
    No / Driver 22 - Yuki Tsunoda
    Competitor Scuderia AlphaTauri
    Time 15:32
    Session Race
    Fact Alleged failure to serve a penalty during a pit stop.
    Infringement Alleged breach of Article 54.4(c) of the FIA Formula One Sporting Regulations.
    Decision 5 second time penalty and 1 penalty point.
    Reason Video evidence showed the car was released unsafely.

    The Stewards:
    Garry Connelly, Mathieu Remmerie, Enrique Bernoldi, Walter Jobst
    """

    fields = parse_decision_fields(text)

    assert fields["car_number"] == 22
    assert fields["driver"] == "Yuki Tsunoda"
    assert fields["team"] == "Scuderia AlphaTauri"
    assert fields["session"] == "Race"
    assert fields["incident_time"] == "15:32"
    assert fields["fact"] == "Alleged failure to serve a penalty during a pit stop."
    assert fields["offence"] == "Alleged breach of Article 54.4(c) of the FIA Formula One Sporting Regulations."
    assert fields["decision"] == "5 second time penalty and 1 penalty point."
    assert fields["reason"] == "Video evidence showed the car was released unsafely."
    assert fields["penalty"] == "5s time penalty"
    assert fields["penalty_points"] == 1
    assert "Article 54.4(c) of the FIA Formula One Sporting Regulations" in fields["rules_referenced"]
    assert fields["stewards"] == ["Garry Connelly", "Mathieu Remmerie", "Enrique Bernoldi", "Walter Jobst"]


def test_image_header_heuristic_uses_position_and_width() -> None:
    assert is_header_image((0, 20, 800, 90), page_width=1000, page_height=1000)
    assert is_header_image((72.9, 36.4, 609.2, 77.0), page_width=1008, page_height=612)
    assert not is_header_image((0, 200, 800, 300), page_width=1000, page_height=1000)
    assert not is_header_image((0, 20, 400, 180), page_width=1000, page_height=1000)
    assert not is_header_image((0, 0, 595, 827), page_width=595, page_height=827)
    assert not is_header_image((205.2, 12.7, 390.0, 114.3), page_width=595, page_height=842)


def test_tiny_image_heuristic_filters_driver_flags_not_real_content() -> None:
    assert is_tiny_non_content_image((156.9, 191.1, 170.2, 199.1), page_width=595, page_height=842)
    assert not is_tiny_non_content_image((55.5, 282.2, 547.4, 630.2), page_width=595, page_height=842)


def test_native_image_heuristic_filters_text_fills_not_photos() -> None:
    assert is_low_value_native_image(
        (93.0, 746.4, 147.3, 752.6),
        page_width=595,
        page_height=842,
        native_width=2,
        native_height=2,
        byte_count=89,
    )
    assert not is_low_value_native_image(
        (72.0, 88.2, 769.2, 581.5),
        page_width=842,
        page_height=595,
        native_width=2132,
        native_height=1507,
        byte_count=245303,
    )


def test_image_bbox_merging_deduplicates_and_joins_split_images() -> None:
    duplicate_full_page = [
        (0.0, 0.0, 595.3, 827.0),
        (0.0, 0.0, 595.3, 827.0),
    ]
    assert merge_image_bboxes(duplicate_full_page, page_width=595.3, page_height=827.0) == [(0.0, 0.0, 595.3, 827.0)]

    split_image = [
        (10.0, 200.0, 110.0, 300.0),
        (110.5, 200.0, 210.0, 300.0),
        (300.0, 200.0, 360.0, 260.0),
    ]
    merged = merge_image_bboxes(split_image, page_width=595.3, page_height=827.0)
    assert (10.0, 200.0, 210.0, 300.0) in merged
    assert (300.0, 200.0, 360.0, 260.0) in merged
    assert len(merged) == 2


def test_page_rendered_doc_types_cover_visual_payload_documents() -> None:
    assert should_render_pages("circuit-map")
    assert should_render_pages("car-presentation")
    assert should_render_pages("pirelli-preview")
    assert should_render_pages("procedure")
    assert not should_render_pages("classification")
    assert not should_render_pages("championship-points")
    assert not should_render_pages("entry-list")


def test_pdf_table_normalization_formats_fia_update_tables() -> None:
    raw_table = [
        ["", "", "Updated", "", "Primary reason", "", "Geometric differences compared to", "Brief description"],
        [None, None, "component", None, "for update", None, "previous version", "(min 20, max 100 words)"],
        ["1", "Front Corner", "", "Performance - Flow Conditioning", "", "Revised furniture", "", "Improves flow."],
        [None, None, None, None, None, None, None, "Adds local load."],
    ]

    rows = normalize_pdf_table(raw_table)

    assert rows == [
        ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"],
        [
            "1",
            "Front Corner",
            "Performance - Flow Conditioning",
            "Revised furniture",
            "Improves flow. Adds local load.",
        ],
    ]
    assert "| No. | Updated component | Primary reason for update |" in table_to_markdown(rows)


def test_pdf_table_normalization_formats_update_table_continuations() -> None:
    raw_table = [
        [None, None, None, None, "continued description text"],
        ["5", "Diffuser", "Performance - Local Load", "Revised diffuser", "Improves rear load."],
    ]

    assert normalize_pdf_table(raw_table) == [
        ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"],
        ["", "", "", "", "continued description text"],
        ["5", "Diffuser", "Performance - Local Load", "Revised diffuser", "Improves rear load."],
    ]


def test_table_continuation_merge_appends_split_description_rows() -> None:
    tables = [
        {
            "page": 1,
            "bbox": [0, 100, 500, 400],
            "rows": [
                ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"],
                ["4", "Floor Edge", "Performance", "New edge", "The floor edge starts"],
            ],
        },
        {
            "page": 2,
            "bbox": [0, 100, 500, 400],
            "rows": [
                ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"],
                ["", "", "", "", "and continues here."],
                ["5", "Diffuser", "Performance", "New diffuser", "Diffuser description."],
            ],
        },
    ]

    assert merge_table_continuations(tables) == [
        {
            "page": 1,
            "bbox": [0, 100, 500, 400],
            "rows": [
                ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"],
                ["4", "Floor Edge", "Performance", "New edge", "The floor edge starts and continues here."],
                ["5", "Diffuser", "Performance", "New diffuser", "Diffuser description."],
            ],
        }
    ]


def test_manifest_skip_logic_retries_failures_and_hash_changes() -> None:
    job = PdfJob(Path("documents/2024/test/file.pdf"), "documents/2024/test/file.pdf", "sha256:abc")
    assert needs_extraction(job, {}, force=False)
    assert not needs_extraction(job, {job.relative_path: {"source_hash": "sha256:abc", "success": True}}, force=False)
    assert needs_extraction(job, {job.relative_path: {"source_hash": "sha256:def", "success": True}}, force=False)
    assert needs_extraction(job, {job.relative_path: {"source_hash": "sha256:abc", "success": False}}, force=False)
    assert needs_extraction(job, {job.relative_path: {"source_hash": "sha256:abc", "success": True}}, force=True)


def test_normalize_date_and_title_helpers() -> None:
    assert normalize_date("30 June 2024") == "2024-06-30"
    assert normalize_date("01.12.2019") == "2019-12-01"
    assert humanize_title("decision-car-22-alleged-unsafe-release") == "Decision Car 22 Alleged Unsafe Release"
