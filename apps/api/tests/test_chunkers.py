import json
from pathlib import Path

import pytest


def test_recursive_chunks_match_pre_m4_golden():
    from services.chunkers import ChunkerConfig, chunk
    from services.vectorization import chunk_text

    fixture = Path(__file__).with_name("chunking_recursive_golden_v1.json")
    cases = json.loads(fixture.read_text(encoding="utf-8"))
    assert len(cases) == 20
    for case in cases:
        config = ChunkerConfig(
            chunk_size=case["size"], chunk_overlap=case["overlap"]
        )
        expected = case["chunks"]
        assert [item.text for item in chunk(case["text"], config)] == expected, case["name"]
        assert chunk_text(case["text"], case["size"], case["overlap"]) == expected


class CharacterTokenizer:
    def encode(self, text):
        return [ord(char) for char in text]

    def decode(self, tokens):
        return "".join(chr(token) for token in tokens)


class Utf8ByteTokenizer:
    def encode(self, text):
        return list(text.encode("utf-8"))

    def decode(self, tokens):
        return bytes(tokens).decode("utf-8", errors="strict")


def test_fixed_char_windows_keep_unicode_codepoints_and_overlap():
    from services.chunkers import ChunkerConfig, chunk

    chunks = chunk(
        "a🙂漢b",
        ChunkerConfig(
            strategy="fixed", chunk_size=2, chunk_overlap=1, unit="chars"
        ),
    )
    assert [part.text for part in chunks] == ["a🙂", "🙂漢", "漢b"]
    token_chunks = chunk(
        "a🙂漢b",
        ChunkerConfig(
            strategy="token", chunk_size=2, chunk_overlap=1, unit="tokens"
        ),
        tokenizer=CharacterTokenizer(),
    )
    assert [part.text for part in token_chunks] == ["a🙂", "🙂漢", "漢b"]
    byte_chunks = chunk(
        "A🙂漢Z",
        ChunkerConfig(
            strategy="token", chunk_size=5, chunk_overlap=1, unit="tokens"
        ),
        tokenizer=Utf8ByteTokenizer(),
    )
    assert [part.text for part in byte_chunks] == ["A🙂", "🙂", "漢Z"]


@pytest.mark.parametrize("strategy", ["fixed", "token"])
def test_token_windows_obey_budget_and_repeat_overlap(strategy):
    from services.chunkers import ChunkerConfig, chunk

    config = ChunkerConfig(
        strategy=strategy,
        chunk_size=4,
        chunk_overlap=2,
        unit="tokens",
    )
    chunks = chunk("abcdefgh", config, tokenizer=CharacterTokenizer())
    token_lists = [CharacterTokenizer().encode(part.text) for part in chunks]
    assert all(len(tokens) <= 4 for tokens in token_lists)
    assert token_lists == [
        list(map(ord, "abcd")),
        list(map(ord, "cdef")),
        list(map(ord, "efgh")),
    ]


def test_chunker_empty_whitespace_and_config_validation(monkeypatch):
    from services.chunkers import ChunkerConfig, ChunkerConfigError, chunk

    assert chunk("", ChunkerConfig()) == []
    assert chunk(" \t\n", ChunkerConfig(strategy="fixed")) == []
    with pytest.raises(ChunkerConfigError, match="positive"):
        ChunkerConfig(chunk_size=0)
    with pytest.raises(ChunkerConfigError, match="overlap"):
        ChunkerConfig(chunk_size=5, chunk_overlap=5)
    with pytest.raises(ChunkerConfigError, match="requires unit"):
        ChunkerConfig(strategy="token", unit="chars")
    with pytest.raises(ChunkerConfigError):
        ChunkerConfig(strategy=[])
    with pytest.raises(ChunkerConfigError):
        ChunkerConfig(unit=[])

    import sys

    monkeypatch.setitem(sys.modules, "tiktoken", None)
    with pytest.raises(
        ChunkerConfigError,
        match="token chunking requires tiktoken; install it or choose 'recursive'",
    ):
        chunk(
            "token text",
            ChunkerConfig(strategy="token", chunk_size=4, chunk_overlap=1, unit="tokens"),
        )


def test_markdown_sections_track_heading_paths_and_ignore_fenced_headings():
    from services.chunkers import ChunkerConfig, chunk

    chunks = chunk(
        "# Install\nroot text\n## Linux\nlinux text\n"
        "```md\n# Not a heading\n```\n### Debian\napt text\n"
        "## Windows\nwindows text\n",
        ChunkerConfig(strategy="markdown", chunk_size=128, chunk_overlap=8),
    )
    assert [(part.text, part.section) for part in chunks] == [
        ("root text", "Install"),
        ("linux text\n```md\n# Not a heading\n```", "Install > Linux"),
        ("apt text", "Install > Linux > Debian"),
        ("windows text", "Install > Windows"),
    ]


def test_chunk_text_preserves_custom_splitter_signature():
    from services.vectorization import chunk_text

    assert chunk_text("a|b|c", 20, 2, split_on=lambda text: text.split("|")) == [
        "a\n\nb\n\nc"
    ]


def test_vectorize_records_renders_templates_and_stamps_markdown_sections():
    from services.chunkers import ChunkerConfig
    from services.vectorization import vectorize_records

    assert ChunkerConfig().strategy == "recursive"
    rows = vectorize_records(
        [
            {"id": "valid", "name": "Ada", "body": "Engineer profile"},
            {"id": "missing", "body": "Missing name profile"},
        ],
        model="hash/32",
        skip_chunking=True,
        text_template="{name}: {body}",
        durable_embedding_cache=False,
    )
    assert rows[0]["content"] == "Ada: Engineer profile"
    assert "name" in rows[1]["_df_embed_error"]

    markdown = vectorize_records(
        [{"id": "guide", "content": "# Install\nBody text"}],
        content_column="content",
        model="hash/32",
        chunk_strategy="markdown",
        chunk_size=128,
        chunk_overlap=8,
        durable_embedding_cache=False,
    )
    assert markdown[0]["metadata"]["_df_section"] == "Install"
