//! Hand-written SVG chart: added latency per scenario (concurrency 8),
//! p50 + p99 bars. Two theme variants are rendered; the README embeds
//! them via <picture prefers-color-scheme>, since GitHub does not apply
//! CSS media queries inside committed <img> SVGs reliably.
//!
//! Colors come from the `dataviz` skill's validated categorical palette
//! (`references/palette.md`): p50/p99 are two series, so they take
//! consecutive categorical slots 1 (blue) and 2 (aqua) rather than
//! skipping to a later slot. Both light and dark instances were run
//! through `scripts/validate_palette.js` and pass all six checks (light
//! mode's slot-2 contrast sits in the WARN/relief band, mitigated here by
//! the always-visible value labels printed beside every bar).

use crate::load::Results;

struct Theme {
    text: &'static str,
    subtext: &'static str,
    grid: &'static str,
    p50: &'static str,
    p99: &'static str,
}

const LIGHT: Theme = Theme {
    text: "#0b0b0b",
    subtext: "#52514e",
    grid: "#e1e0d9",
    p50: "#2a78d6",
    p99: "#1baf7a",
};
const DARK: Theme = Theme {
    text: "#ffffff",
    subtext: "#c3c2b7",
    grid: "#2c2c2a",
    p50: "#3987e5",
    p99: "#199e70",
};

pub fn render(results: &Results, dark: bool) -> String {
    let t = if dark { &DARK } else { &LIGHT };
    let cells: Vec<_> = results
        .cells
        .iter()
        .filter(|c| c.concurrency == 8)
        .collect();

    const W: f64 = 720.0;
    const GUTTER: f64 = 130.0; // left label column
    const HEADER: f64 = 56.0; // title + legend
    const ROW_H: f64 = 56.0; // two bars + gap per scenario
    const BAR_H: f64 = 18.0;
    const FOOTER: f64 = 46.0; // axis labels + machine note
    let plot_w = W - GUTTER - 50.0;
    let h = HEADER + cells.len() as f64 * ROW_H + FOOTER;

    // Axis max: largest value rounded up to a clean 1-2-5 step, >= 4 ticks.
    let max_val = cells
        .iter()
        .flat_map(|c| [c.added_p50_ms, c.added_p99_ms])
        .fold(0.1_f64, f64::max);
    let step = [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]
        .into_iter()
        .find(|s| max_val <= s * 4.0)
        .unwrap_or(100.0);
    let axis_max = (max_val / step).ceil() * step;
    let x = |v: f64| GUTTER + v / axis_max * plot_w;

    let mut s = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {W} {h}\" \
         font-family=\"-apple-system, 'Segoe UI', sans-serif\" font-size=\"12\">\n"
    );
    s.push_str(&format!(
        "<text x=\"16\" y=\"24\" font-size=\"15\" font-weight=\"600\" fill=\"{}\">\
         Added latency vs. direct upstream (ms, concurrency 8)</text>\n",
        t.text
    ));
    // Legend: two swatches on the header line.
    s.push_str(&format!(
        "<rect x=\"16\" y=\"36\" width=\"12\" height=\"12\" rx=\"2\" fill=\"{}\"/>\
         <text x=\"33\" y=\"46\" fill=\"{}\">p50</text>\n",
        t.p50, t.subtext
    ));
    s.push_str(&format!(
        "<rect x=\"70\" y=\"36\" width=\"12\" height=\"12\" rx=\"2\" fill=\"{}\"/>\
         <text x=\"87\" y=\"46\" fill=\"{}\">p99</text>\n",
        t.p99, t.subtext
    ));
    // Vertical gridlines + tick labels.
    let mut tick = 0.0;
    while tick <= axis_max + 1e-9 {
        let tx = x(tick);
        s.push_str(&format!(
            "<line x1=\"{tx:.1}\" y1=\"{HEADER}\" x2=\"{tx:.1}\" y2=\"{:.1}\" \
             stroke=\"{}\" stroke-width=\"1\"/>\n",
            h - FOOTER + 4.0,
            t.grid
        ));
        s.push_str(&format!(
            "<text x=\"{tx:.1}\" y=\"{:.1}\" text-anchor=\"middle\" fill=\"{}\">{tick}</text>\n",
            h - FOOTER + 18.0,
            t.subtext
        ));
        tick += step;
    }
    // One row per scenario: label + p50/p99 bars with value annotations.
    for (i, c) in cells.iter().enumerate() {
        let top = HEADER + i as f64 * ROW_H + 8.0;
        s.push_str(&format!(
            "<text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\" fill=\"{}\">{}</text>\n",
            GUTTER - 10.0,
            top + BAR_H + 2.0,
            t.text,
            c.scenario
        ));
        for (j, (v, color)) in [(c.added_p50_ms, t.p50), (c.added_p99_ms, t.p99)]
            .iter()
            .enumerate()
        {
            let y = top + j as f64 * (BAR_H + 4.0);
            let wpx = (x(*v) - GUTTER).max(1.0);
            s.push_str(&format!(
                "<rect x=\"{GUTTER}\" y=\"{y:.1}\" width=\"{wpx:.1}\" height=\"{BAR_H}\" \
                 rx=\"2\" fill=\"{color}\"/>\n"
            ));
            s.push_str(&format!(
                "<text x=\"{:.1}\" y=\"{:.1}\" fill=\"{}\">{v:.2}</text>\n",
                GUTTER + wpx + 6.0,
                y + BAR_H - 4.0,
                t.subtext
            ));
        }
    }
    // Footer: machine + version + date label (transparency: which box).
    s.push_str(&format!(
        "<text x=\"16\" y=\"{:.1}\" fill=\"{}\">{}/{} · {} cpus · sluice v{} · {}</text>\n",
        h - 10.0,
        t.subtext,
        results.machine.os,
        results.machine.arch,
        results.machine.cpus,
        results.sluice_version,
        results.date
    ));
    s.push_str("</svg>\n");
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::{Cell, Machine, Results};
    use crate::stats::Summary;

    fn fake_results() -> Results {
        let s = |p50: f64| Summary {
            count: 100,
            rps: 500.0,
            p50_ms: p50,
            p95_ms: p50 * 2.0,
            p99_ms: p50 * 3.0,
        };
        Results {
            sluice_version: "0.1.0".into(),
            date: "2026-07-10".into(),
            machine: Machine {
                os: "macos".into(),
                arch: "aarch64".into(),
                cpus: 8,
            },
            cells: vec![Cell {
                scenario: "passthrough".into(),
                concurrency: 8,
                sluice: s(3.0),
                baseline: s(2.5),
                added_p50_ms: 0.5,
                added_p99_ms: 1.5,
                ttfb_p50_ms: None,
                sluice_errors: 0,
                baseline_errors: 0,
            }],
        }
    }

    #[test]
    fn renders_valid_svg_with_scenario_labels() {
        let svg = render(&fake_results(), false);
        assert!(svg.starts_with("<svg"));
        assert!(svg.ends_with("</svg>\n"));
        assert!(svg.contains("passthrough"));
        assert!(svg.contains("0.5")); // the added-p50 value appears
    }

    #[test]
    fn dark_variant_uses_light_text() {
        let light = render(&fake_results(), false);
        let dark = render(&fake_results(), true);
        assert_ne!(light, dark);
    }
}
