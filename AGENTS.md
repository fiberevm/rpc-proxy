General coding
- Do not normalize inputs. Assume parameters already have the expected shape. When validation is required, validate them explicitly with the project’s chosen validation library.
- Do not use resolve in names; use get instead.
- Keep shared utilities in the utils package. Avoid standalone helper functions in feature packages; prefer methods on the relevant service type.
- Add concise, useful comments throughout non-obvious code. Add a documentation comment to every exported function and method explaining why it exists, what it does, and what inputs it accepts.
- Prefer the Go standard library for slice, map, string, and collection operations. Introduce a third-party utility package only when it materially improves clarity.
- Validate inputs inside the function or method that uses them, rather than in a separate validation helper.
- Keep code DRY and avoid duplicate or boilerplate implementations.
- Minimize logic outside service types.
- Return (T, error) from backend operations. Use meaningful sentinel or typed errors when callers need to distinguish failure cases.
- When a function has several related inputs, accept a named parameter struct instead of a long positional argument list. Access fields directly and avoid vague parameter names such as input or params when a more specific name is available.
Services
- Do not access the database directly outside the service layer.
- Put database- and vendor-oriented operations in the services package or its feature-specific subpackages.
- Inject database and vendor dependencies into service types rather than relying on package-level mutable state.
- Keep interfaces small and define them near the code that consumes them.
Tests
- Minimize mocking.
- Prefer constructing complete domain values—such as users, seeds, and key derivations—rather than mocking their packages.
- Test observable behavior and overall functionality rather than small implementation details.
- When replacing service behavior, inject a small interface or function dependency and use a test implementation.
- Prefer table-driven tests when several cases exercise the same behavior.
- Use t.Helper() in reusable test helpers.
- Avoid asserting on internal calls unless those calls are part of the required behavior.
Code quality
- Use explicit variable names. Avoid vague names such as result, resolved, ensured, data, or value; prefer contextual names such as user, userErr, or response.
- Separate logically distinct parts of a function with empty lines.
- Use explicit but reasonably concise function and method names.
- Handle every error explicitly. Wrap errors with useful context using fmt.Errorf("context: %w", err).
- Avoid package-level mutable state.
- Keep packages focused and avoid generic catch-all abstractions.
- Follow standard Go formatting and linting conventions. Run gofmt and the project’s configured tests and linters before considering a change complete.
Environment variables
- Do not read or validate environment variables inside services.
- Define, load, and validate every environment variable in the central configuration package.
- Convert environment variables into typed configuration values during application startup.
- Inject validated configuration into the services that need it.
- Fail startup with a clear error when required configuration is missing or invalid.