from typer.testing import CliRunner

from recsys_tfb.__main__ import app

runner = CliRunner()


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "--pipeline" in result.output
        assert "--env" in result.output

    def test_help_shows_options(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Pipeline name to run" in result.output

    def test_unknown_pipeline(self, tmp_path):
        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)

        import os

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "nonexistent"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)

    def test_successful_run_empty_pipeline(self, tmp_path):
        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)

        import os

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "dataset"])
            assert result.exit_code == 0
        finally:
            os.chdir(old_cwd)
