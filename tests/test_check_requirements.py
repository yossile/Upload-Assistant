# ruff: noqa: S101
import importlib.metadata

import packaging.markers
import pytest

from src import check_requirements


@pytest.fixture(autouse=True)
def isolated_dependency_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(check_requirements, "DEPENDENCY_CACHE_PATH", tmp_path / "cache" / "dependency-check.json")


@pytest.fixture
def dependency_environment(tmp_path, monkeypatch):
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("example==1.0\n", encoding="utf-8")
    monkeypatch.setattr(check_requirements, "__file__", str(source_dir / "check_requirements.py"))
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    monkeypatch.setattr(check_requirements.sys, "path", [str(site_packages)])
    calls = []

    def installed_version(name):
        calls.append(name)
        return "1.0"

    monkeypatch.setattr(check_requirements.importlib.metadata, "version", installed_version)
    return requirements_file, site_packages, calls


def test_check_dependencies_reuses_successful_check(dependency_environment):
    _, _, calls = dependency_environment

    check_requirements.check_dependencies()
    check_requirements.check_dependencies()

    assert calls == ["example"]


def test_check_dependencies_rechecks_changed_requirements(dependency_environment):
    requirements_file, _, calls = dependency_environment
    check_requirements.check_dependencies()

    requirements_file.write_text("example==1.0\nother==1.0\n", encoding="utf-8")
    check_requirements.check_dependencies()

    assert calls == ["example", "example", "other"]


def test_check_dependencies_rechecks_changed_environment(dependency_environment):
    _, site_packages, calls = dependency_environment
    check_requirements.check_dependencies()

    (site_packages / "other-1.0.dist-info").mkdir()
    check_requirements.check_dependencies()

    assert calls == ["example", "example"]


def test_check_dependencies_can_force_recheck(dependency_environment):
    _, _, calls = dependency_environment
    check_requirements.check_dependencies()
    check_requirements.check_dependencies(force=True)

    assert calls == ["example", "example"]


def test_check_dependencies_rechecks_corrupt_cache(dependency_environment):
    _, _, calls = dependency_environment
    check_requirements.check_dependencies()

    check_requirements.DEPENDENCY_CACHE_PATH.write_text("{", encoding="utf-8")
    check_requirements.check_dependencies()

    assert calls == ["example", "example"]


def test_check_dependencies_does_not_cache_mismatch(dependency_environment, capsys):
    requirements_file, _, calls = dependency_environment
    requirements_file.write_text("example==2.0\n", encoding="utf-8")

    check_requirements.check_dependencies()
    check_requirements.check_dependencies()

    assert calls == ["example", "example"]
    assert not check_requirements.DEPENDENCY_CACHE_PATH.exists()
    assert capsys.readouterr().out.count("Mismatched/Outdated Versions:") == 2


def test_check_dependencies_does_not_cache_missing_package(dependency_environment, monkeypatch, capsys):
    _, _, calls = dependency_environment

    def missing_version(name):
        calls.append(name)
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(check_requirements.importlib.metadata, "version", missing_version)

    for _ in range(2):
        with pytest.raises(SystemExit, match="1"):
            check_requirements.check_dependencies()

    assert calls == ["example", "example"]
    assert not check_requirements.DEPENDENCY_CACHE_PATH.exists()
    assert capsys.readouterr().out.count("Missing Packages:") == 2


def test_check_dependencies_forced_failure_discards_previous_success(dependency_environment, monkeypatch, capsys):
    _, _, calls = dependency_environment
    check_requirements.check_dependencies()

    def mismatched_version(name):
        calls.append(name)
        return "2.0"

    monkeypatch.setattr(check_requirements.importlib.metadata, "version", mismatched_version)
    check_requirements.check_dependencies(force=True)
    check_requirements.check_dependencies()

    assert calls == ["example", "example", "example"]
    assert not check_requirements.DEPENDENCY_CACHE_PATH.exists()
    assert capsys.readouterr().out.count("Mismatched/Outdated Versions:") == 2


def test_check_dependencies_rechecks_changed_python(dependency_environment, monkeypatch):
    _, _, calls = dependency_environment
    check_requirements.check_dependencies()

    monkeypatch.setattr(check_requirements.sys, "version", "different Python build")
    check_requirements.check_dependencies()

    assert calls == ["example", "example"]


def test_check_dependencies_tolerates_unwritable_cache(dependency_environment, monkeypatch):
    requirements_file, _, calls = dependency_environment
    monkeypatch.setattr(check_requirements, "DEPENDENCY_CACHE_PATH", requirements_file / "cache.json")

    check_requirements.check_dependencies()
    check_requirements.check_dependencies()

    assert calls == ["example", "example"]


def test_check_dependencies_skips_requirement_with_nonmatching_marker(tmp_path, monkeypatch, capsys):
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    (tmp_path / "requirements.txt").write_text('pywinpty==3.0.5 ; sys_platform == "win32"\n', encoding="utf-8")
    monkeypatch.setattr(check_requirements, "__file__", str(source_dir / "check_requirements.py"))

    environment = packaging.markers.default_environment()
    environment["sys_platform"] = "linux"
    monkeypatch.setattr(packaging.markers, "default_environment", lambda: environment)

    def missing_pywinpty(name):
        assert name == "pywinpty"
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(check_requirements.importlib.metadata, "version", missing_pywinpty)

    check_requirements.check_dependencies()

    assert capsys.readouterr().out == ""
