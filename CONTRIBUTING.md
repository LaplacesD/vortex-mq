# Contributing to vortex-mq

We welcome contributions! Here's how to get started.

## Development Setup

```bash
git clone https://github.com/LaplacesD/vortex-mq.git
cd vortex-mq
pip install -e ".[dev]"
```

## Running Tests

```bash
pytest tests/ -v
```

## Code Style

We use `ruff` for linting:

```bash
pip install ruff
ruff check vortex/ tests/
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest tests/ -v`)
5. Commit with a descriptive message
6. Push to your fork
7. Open a Pull Request

## Commit Messages

Use conventional commits:
- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation
- `test:` — tests
- `refactor:` — code restructuring
- `chore:` — maintenance

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
