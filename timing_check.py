import csv
import time
import requests
import statistics

BASE_URL = "https://asvc-qcs-website-01-eub8gdbpghf7aaeu.qatarcentral-01.azurewebsites.net"
ITERATIONS = 7
TIMEOUT = 20


HEADERS = {
    "User-Agent": "MoJ-SQLi-Verification/1.0"
}


def measure(url):
    times = []

    for _ in range(ITERATIONS):
        start = time.perf_counter()

        r = requests.get(
            BASE_URL + url,
            headers=HEADERS,
            timeout=TIMEOUT,
            verify=False
        )

        r.raise_for_status()

        end = time.perf_counter()
        times.append((end - start) * 1000)

    return times


def main():
    results = []

    with open("timing-check-input.csv") as f:
        reader = csv.DictReader(f)

        for row in reader:
            name = row["name"]

            print(f"Testing: {name}")

            try:
                # Warm-up
                requests.get(BASE_URL + row["normal_url"], verify=False)

                normal_times = measure(row["normal_url"])
                inject_times = measure(row["inject_url"])

                normal_avg = statistics.mean(normal_times)
                inject_avg = statistics.mean(inject_times)

                delta = inject_avg - normal_avg

                verdict = "Not Exploitable"
                if delta > 500:
                    verdict = "Potential Risk"

                results.append([
                    name,
                    round(normal_avg, 2),
                    round(inject_avg, 2),
                    round(delta, 2),
                    verdict
                ])
            except requests.RequestException as e:
                print(f"Error testing {name}: {e}")
                results.append([name, "ERROR", "ERROR", "ERROR", str(e)])

    with open("timing-check-report.csv", "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "Endpoint",
            "Normal_Avg_ms",
            "Injected_Avg_ms",
            "Delta_ms",
            "Verdict"
        ])

        writer.writerows(results)

    print("Report saved as timing-check-report.csv")


if __name__ == "__main__":
    main()
