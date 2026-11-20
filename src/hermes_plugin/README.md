# Hermes hcom plugin

This directory holds the Hermes-side plugin formerly embedded in the hcom
repository. It is independent of the hcom build and is not installed, enabled,
updated, or removed by `hcom`.

To use the bridge, install `__init__.py` and `plugin.yaml` as a Hermes user
plugin named `hcom` (normally under `~/.hermes/plugins/hcom/`), then enable it
with `hermes plugins enable hcom`. The plugin invokes the hcom CLI through
`HCOM_BIN` or `PATH`; hcom stores its own state under `HCOM_DIR` when set.

`dashboard/` is a separate Hermes Desktop UI experiment. It is not installed
by the bridge and may require Hermes Desktop plugin tooling and Python
dependencies (`fastapi`, `pydantic`).

Run the bridge tests with `bash test.sh`.
