# ruff: noqa: S101
import asyncio
import io
import logging
from datetime import datetime
from io import StringIO

from rich.console import Console

from src.console import _format_log_time, _safe_input, ansi_to_html, buffer_console_logs, enable_console_timestamps, prompt_in_thread, rich_handler


class ByteInput:
    def __init__(self, value: bytes) -> None:
        self.buffer = io.BytesIO(value)


def test_safe_input_ignores_dangling_utf8_byte_after_multibyte_character_is_deleted(monkeypatch, capsys) -> None:
    monkeypatch.setattr("src.console.sys.stdin", ByteInput(b"\xd7y\n"))

    assert _safe_input("> ") == "y"
    assert capsys.readouterr().out == "> "


def test_ansi_to_html_preserves_osc8_hyperlinks() -> None:
    stream = StringIO()
    Console(file=stream, force_terminal=True, color_system="truecolor", legacy_windows=False).print("[link=https://example.test]link[/link]")

    html = ansi_to_html(stream.getvalue())

    assert '<a href="https://example.test">link</a>' in html


def test_ansi_to_html_does_not_echo_to_server_output(capsys) -> None:
    html = ansi_to_html("\x1b[31mExample output\x1b[0m")

    assert "Example output" in html
    assert "color:" in html
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_prompt_in_thread_returns_prompt_result() -> None:
    async def ask() -> str:
        return await prompt_in_thread(lambda prefix, value: f"{prefix}{value}", "answer-", 42)

    assert asyncio.run(ask()) == "answer-42"


def test_log_time_format_includes_milliseconds() -> None:
    timestamp = datetime(2026, 10, 14, 12, 34, 56, 123456)

    assert _format_log_time(timestamp).plain == "12:34:56.123"


def test_console_timestamps_appear_on_repeated_log_times(monkeypatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(rich_handler, "console", Console(file=stream, width=120))
    monkeypatch.setattr(rich_handler._log_render, "show_time", False)
    enable_console_timestamps()
    record = logging.LogRecord("test", logging.INFO, "test", 1, "Stage completed", (), None)
    record.created = 1234567890.123
    rich_handler.emit(record)
    rich_handler.emit(record)

    timestamp = _format_log_time(datetime.fromtimestamp(record.created)).plain
    assert stream.getvalue().count(timestamp) == 2


def test_buffer_console_logs_can_contend_across_consecutive_event_loops() -> None:
    async def contend_for_buffer() -> None:
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        second_started = asyncio.Event()
        order: list[str] = []

        async def first_user() -> None:
            async with buffer_console_logs():
                order.append("first-entered")
                first_entered.set()
                await release_first.wait()
                order.append("first-leaving")

        async def second_user() -> None:
            await first_entered.wait()
            second_started.set()
            async with buffer_console_logs():
                order.append("second-entered")

        first_task = asyncio.create_task(first_user())
        second_task = asyncio.create_task(second_user())
        await second_started.wait()
        await asyncio.sleep(0)
        release_first.set()
        await asyncio.gather(first_task, second_task)

        assert order == ["first-entered", "first-leaving", "second-entered"]

    asyncio.run(contend_for_buffer())
    asyncio.run(contend_for_buffer())
