"""Start the single local Airflow service after container recreation."""
import os
from pathlib import Path


def main():
    home = Path(os.environ['AIRFLOW_HOME']).resolve()
    home.mkdir(parents=True, exist_ok=True)
    # A PID saved by an earlier container can identify a different process in
    # the new container and incorrectly prevent the webserver from starting.
    (home / 'airflow-webserver.pid').unlink(missing_ok=True)
    os.execvp('airflow', ['airflow', 'standalone'])


if __name__ == '__main__':
    main()
