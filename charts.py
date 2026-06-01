import matplotlib.pyplot as plt

protocols = ["HTTP/1.1", "gRPC", "WebSocket", "MQTT Q0", "MQTT Q1", "MQTT Q2"]
colors = ["#C0392B", "#028090", "#F59E0B", "#22C55E", "#16A34A", "#15803D"]

conditions = {
    "Normal":        [213,  3870, 1231, 4940, 2732, 2621],
    "+200ms RTT":    [4.5,  2618,  4.8, 2406,   44,   33],
    "5% Loss":       [296,  3958, 2530, 8341, 2983,  890],
    "Bandwidth Cap": [14.7,  258,  192,   33,   33,   33],
}

for title, values in conditions.items():
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(protocols, values, color=colors, width=0.5)

    # value labels on top of each bar
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + max(values)*0.01,
                str(val), ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_title(f"Throughput — {title}", fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel("Throughput (msg/s)")
    ax.set_xlabel("Protocol")
    ax.tick_params(axis='x', rotation=15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    filename = title.replace(" ", "_").replace("/", "").replace("+", "plus") + ".png"
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"Saved: {filename}")