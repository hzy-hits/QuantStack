"""Report renderer entry points (Phase C of REFACTOR_PLAN.md).

Each module here owns one top-level render_*_report function. They use a
lazy proxy to the main monolith for symbols not yet extracted, so they
can be imported by the main monolith without circular-import issues.
"""
