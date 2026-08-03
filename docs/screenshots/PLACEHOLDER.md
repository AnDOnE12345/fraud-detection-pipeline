# Screenshots (evidence for grading)

Place the required screenshots here and embed them in the top-level `README.md`
(section 11 "Screenshots und Nachweise").

Capture these on the second laptop after `helm install`:

1. `pods.png` — output of `kubectl get pods` showing all components Running.
2. `ui-producer.png` — the web UI submitting a transaction (data-provider role).
3. `ui-dashboard.png` — the dashboard showing flagged vs. approved transactions.
4. `serving-api.png` — a serving API response (e.g. `GET /stats` or `/flagged`).
5. `pipeline-output.png` — processor logs / example Delta output (Bronze/Silver/Gold).
6. `scaling.png` — `kubectl get hpa` or `kubectl scale ...` showing horizontal scaling.

Keep file names stable so the README links do not break.
