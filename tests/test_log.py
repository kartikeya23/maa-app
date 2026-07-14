import logging
import logging.handlers

import pytest

import db
import log


@pytest.fixture(autouse=True)
def clean_maa_logger():
    """Reset the 'maa' logger around each test so setup_logging starts fresh."""
    maa = logging.getLogger("maa")
    saved = maa.handlers[:]
    maa.handlers.clear()
    yield
    for h in maa.handlers:
        h.close()
    maa.handlers = saved


def test_file_handler_config(tmp_path):
    logger = log.setup_logging(log_file=tmp_path / "maa.log")
    handlers = [h for h in logger.handlers
                if isinstance(h, logging.handlers.TimedRotatingFileHandler)]
    assert len(handlers) == 1
    fh = handlers[0]
    assert fh.backupCount == 14
    assert fh.when == "MIDNIGHT"


def test_idempotent(tmp_path):
    log.setup_logging(log_file=tmp_path / "maa.log")
    logger = log.setup_logging(log_file=tmp_path / "maa.log")
    assert len(logger.handlers) == 1  # no duplicate handlers on second call


def test_console_handler_only_when_requested(tmp_path):
    logger = log.setup_logging(console=True, log_file=tmp_path / "maa.log")
    stream_handlers = [h for h in logger.handlers
                       if type(h) is logging.StreamHandler]
    assert len(stream_handlers) == 1
    assert stream_handlers[0].formatter._fmt == "%(message)s"


def test_default_level_is_info(tmp_path):
    logger = log.setup_logging(log_file=tmp_path / "maa.log")
    assert logger.level == logging.INFO


def test_verbose_sets_debug(tmp_path):
    logger = log.setup_logging(verbose=True, log_file=tmp_path / "maa.log")
    assert logger.level == logging.DEBUG


def test_env_var_sets_debug(tmp_path, monkeypatch):
    monkeypatch.setenv("MAA_DEBUG", "1")
    logger = log.setup_logging(log_file=tmp_path / "maa.log")
    assert logger.level == logging.DEBUG


def test_writes_to_file(tmp_path):
    log_file = tmp_path / "maa.log"
    logger = log.setup_logging(log_file=log_file)
    logging.getLogger("maa.test").info("hello %s", "world")
    for h in logger.handlers:
        h.flush()
    content = log_file.read_text()
    assert "hello world" in content
    assert "INFO maa.test:" in content
