
Application Usage
A table showing the number of installs in vscode.
A line graph showing the frequency of the number of times used each day.


A table showing installs per extension version together with a suitable chart showing this information.

A table showing tallies for which operating system is used.  Grab this when installed.  Show these as a pie chart.   IT will need to read the operating systems as collected from installs and these should not be hard coded as are user depenedent.

Implemented as counters-only telemetry:
- Backend now tallies installs total, daily installs, installs by extension version, by OS, and by country.
- New endpoint: GET /stats/install returns { installsTotal, dailyInstalls{dates,counts}, byExt, byOs, byCountry }.
- Dashboard page: dashboard/installs.html renders the above (total KPI, daily line, versions bar, OS pie, country table).
- Ingest only accepts events: install.created and extension.upgraded; other events are ignored.

