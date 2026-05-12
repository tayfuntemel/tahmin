<?php
session_start();
date_default_timezone_set('Europe/Istanbul');

require_once 'config.php';

try {
    $pdo->exec("
        CREATE TABLE IF NOT EXISTS user_prediction_settings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            min_confidence DECIMAL(5,4) NOT NULL DEFAULT 0.6800,
            show_goals TINYINT(1) NOT NULL DEFAULT 1,
            show_btts TINYINT(1) NOT NULL DEFAULT 1,
            show_corners TINYINT(1) NOT NULL DEFAULT 1,
            show_shots TINYINT(1) NOT NULL DEFAULT 1,
            show_fouls TINYINT(1) NOT NULL DEFAULT 1,
            show_team_goals TINYINT(1) NOT NULL DEFAULT 1,
            base_unit DECIMAL(12,2) NOT NULL DEFAULT 1000.00,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_session (session_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ");
} catch (PDOException $e) {
    die("veritabanı bağlantı hatası: " . mb_strtolower($e->getMessage(), 'UTF-8'));
}

if (!isset($_COOKIE['ns_google_id']) || trim($_COOKIE['ns_google_id']) === '') {
    header('Location: index.php');
    exit;
}

$session_id = $_COOKIE['ns_google_id'];

function e($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function lower_tr($value) {
    return mb_strtolower((string)$value, 'UTF-8');
}

function marketLabel($selection) {
    $map = [
        'over_1_5' => '1.5 üst',
        'over_2_5' => '2.5 üst',
        'under_3_5' => '3.5 alt',

        'btts_yes' => 'kg var',
        'btts_no' => 'kg yok',

        'corners_over_8_5' => 'korner 8.5 üst',
        'corners_over_9_5' => 'korner 9.5 üst',
        'corners_under_11_5' => 'korner 11.5 alt',

        'shots_over_20_5' => 'şut 20.5 üst',
        'shots_over_22_5' => 'şut 22.5 üst',
        'shots_on_target_over_6_5' => 'isabetli şut 6.5 üst',

        'fouls_over_24_5' => 'faul 24.5 üst',
        'fouls_under_28_5' => 'faul 28.5 alt',

        'home_over_0_5' => 'ev sahibi 0.5 üst gol',
        'away_over_0_5' => 'deplasman 0.5 üst gol',
    ];

    return $map[$selection] ?? str_replace('_', ' ', $selection);
}

function checkPredictionResult($selection, $row) {
    $status = lower_tr($row['match_status'] ?? $row['status'] ?? '');

    if (!in_array($status, ['finished', 'ended'], true)) {
        return 'pending';
    }

    $ft_h = isset($row['ft_home']) && $row['ft_home'] !== null ? (int)$row['ft_home'] : null;
    $ft_a = isset($row['ft_away']) && $row['ft_away'] !== null ? (int)$row['ft_away'] : null;

    if ($ft_h === null || $ft_a === null) {
        return 'pending';
    }

    $total_goals = $ft_h + $ft_a;

    $corn_h = $row['corn_h'] !== null ? (int)$row['corn_h'] : null;
    $corn_a = $row['corn_a'] !== null ? (int)$row['corn_a'] : null;

    $shot_h = $row['shot_h'] !== null ? (int)$row['shot_h'] : null;
    $shot_a = $row['shot_a'] !== null ? (int)$row['shot_a'] : null;

    $shot_on_h = $row['shot_on_h'] !== null ? (int)$row['shot_on_h'] : null;
    $shot_on_a = $row['shot_on_a'] !== null ? (int)$row['shot_on_a'] : null;

    $fouls_h = $row['fouls_h'] !== null ? (int)$row['fouls_h'] : null;
    $fouls_a = $row['fouls_a'] !== null ? (int)$row['fouls_a'] : null;

    $total_corners = ($corn_h !== null && $corn_a !== null) ? $corn_h + $corn_a : null;
    $total_shots = ($shot_h !== null && $shot_a !== null) ? $shot_h + $shot_a : null;
    $total_shots_on = ($shot_on_h !== null && $shot_on_a !== null) ? $shot_on_h + $shot_on_a : null;
    $total_fouls = ($fouls_h !== null && $fouls_a !== null) ? $fouls_h + $fouls_a : null;

    switch ($selection) {
        case 'over_1_5':
            return $total_goals >= 2 ? 'win' : 'lose';

        case 'over_2_5':
            return $total_goals >= 3 ? 'win' : 'lose';

        case 'under_3_5':
            return $total_goals <= 3 ? 'win' : 'lose';

        case 'btts_yes':
            return ($ft_h > 0 && $ft_a > 0) ? 'win' : 'lose';

        case 'btts_no':
            return ($ft_h === 0 || $ft_a === 0) ? 'win' : 'lose';

        case 'corners_over_8_5':
            return $total_corners === null ? 'pending' : ($total_corners >= 9 ? 'win' : 'lose');

        case 'corners_over_9_5':
            return $total_corners === null ? 'pending' : ($total_corners >= 10 ? 'win' : 'lose');

        case 'corners_under_11_5':
            return $total_corners === null ? 'pending' : ($total_corners <= 11 ? 'win' : 'lose');

        case 'shots_over_20_5':
            return $total_shots === null ? 'pending' : ($total_shots >= 21 ? 'win' : 'lose');

        case 'shots_over_22_5':
            return $total_shots === null ? 'pending' : ($total_shots >= 23 ? 'win' : 'lose');

        case 'shots_on_target_over_6_5':
            return $total_shots_on === null ? 'pending' : ($total_shots_on >= 7 ? 'win' : 'lose');

        case 'fouls_over_24_5':
            return $total_fouls === null ? 'pending' : ($total_fouls >= 25 ? 'win' : 'lose');

        case 'fouls_under_28_5':
            return $total_fouls === null ? 'pending' : ($total_fouls <= 28 ? 'win' : 'lose');

        case 'home_over_0_5':
            return $ft_h >= 1 ? 'win' : 'lose';

        case 'away_over_0_5':
            return $ft_a >= 1 ? 'win' : 'lose';

        default:
            return 'pending';
    }
}

function getSettings($pdo, $session_id) {
    $pdo->prepare("
        INSERT IGNORE INTO user_prediction_settings (session_id)
        VALUES (?)
    ")->execute([$session_id]);

    $stmt = $pdo->prepare("SELECT * FROM user_prediction_settings WHERE session_id = ?");
    $stmt->execute([$session_id]);

    return $stmt->fetch(PDO::FETCH_ASSOC);
}

function enabledMarketsFromSettings($settings) {
    $markets = [];

    if ((int)$settings['show_goals'] === 1) $markets[] = 'goals';
    if ((int)$settings['show_btts'] === 1) $markets[] = 'btts';
    if ((int)$settings['show_corners'] === 1) $markets[] = 'corners';
    if ((int)$settings['show_shots'] === 1) $markets[] = 'shots';
    if ((int)$settings['show_fouls'] === 1) $markets[] = 'fouls';
    if ((int)$settings['show_team_goals'] === 1) $markets[] = 'team_goals';

    return $markets;
}

$settings = getSettings($pdo, $session_id);

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action'])) {
    header('Content-Type: application/json; charset=utf-8');

    if ($_POST['action'] === 'save_settings') {
        $min_confidence = isset($_POST['min_confidence']) ? (float)$_POST['min_confidence'] : 0.68;

        if ($min_confidence < 0.50) $min_confidence = 0.50;
        if ($min_confidence > 0.95) $min_confidence = 0.95;

        $show_goals = isset($_POST['show_goals']) ? 1 : 0;
        $show_btts = isset($_POST['show_btts']) ? 1 : 0;
        $show_corners = isset($_POST['show_corners']) ? 1 : 0;
        $show_shots = isset($_POST['show_shots']) ? 1 : 0;
        $show_fouls = isset($_POST['show_fouls']) ? 1 : 0;
        $show_team_goals = isset($_POST['show_team_goals']) ? 1 : 0;

        $base_unit = isset($_POST['base_unit']) ? (float)$_POST['base_unit'] : 1000;
        if ($base_unit <= 0) $base_unit = 1000;

        $stmt = $pdo->prepare("
            INSERT INTO user_prediction_settings (
                session_id,
                min_confidence,
                show_goals,
                show_btts,
                show_corners,
                show_shots,
                show_fouls,
                show_team_goals,
                base_unit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                min_confidence = VALUES(min_confidence),
                show_goals = VALUES(show_goals),
                show_btts = VALUES(show_btts),
                show_corners = VALUES(show_corners),
                show_shots = VALUES(show_shots),
                show_fouls = VALUES(show_fouls),
                show_team_goals = VALUES(show_team_goals),
                base_unit = VALUES(base_unit),
                updated_at = CURRENT_TIMESTAMP
        ");

        $stmt->execute([
            $session_id,
            $min_confidence,
            $show_goals,
            $show_btts,
            $show_corners,
            $show_shots,
            $show_fouls,
            $show_team_goals,
            $base_unit,
        ]);

        echo json_encode(['status' => 'success']);
        exit;
    }

    if ($_POST['action'] === 'reset_settings') {
        $stmt = $pdo->prepare("
            UPDATE user_prediction_settings
            SET
                min_confidence = 0.6800,
                show_goals = 1,
                show_btts = 1,
                show_corners = 1,
                show_shots = 1,
                show_fouls = 1,
                show_team_goals = 1,
                base_unit = 1000.00,
                updated_at = CURRENT_TIMESTAMP
            WHERE session_id = ?
        ");
        $stmt->execute([$session_id]);

        echo json_encode(['status' => 'success']);
        exit;
    }

    if ($_POST['action'] === 'calculate_simulation') {
        $settings = getSettings($pdo, $session_id);
        $enabled_markets = enabledMarketsFromSettings($settings);

        if (empty($enabled_markets)) {
            echo json_encode([
                'status' => 'success',
                'total' => 0,
                'won' => 0,
                'lost' => 0,
                'pending' => 0,
                'hit_rate' => null,
                'unit_profit' => 0,
                'money_profit' => 0,
                'labels' => [],
                'data' => [],
                'max_daily_bets' => 0,
            ]);
            exit;
        }

        $base_unit = isset($_POST['base_unit']) ? (float)$_POST['base_unit'] : (float)$settings['base_unit'];
        if ($base_unit <= 0) $base_unit = 1000;

        $placeholders = implode(',', array_fill(0, count($enabled_markets), '?'));

        $query = "
            SELECT
                P.*,
                R.status AS match_status,
                R.ft_home,
                R.ft_away,
                R.corn_h,
                R.corn_a,
                R.shot_h,
                R.shot_a,
                R.shot_on_h,
                R.shot_on_a,
                R.fouls_h,
                R.fouls_a
            FROM predictions_football P
            JOIN results_football R ON P.event_id = R.event_id
            WHERE R.status IN ('finished', 'ended')
              AND P.confidence_score >= ?
              AND P.market_type IN ($placeholders)
            ORDER BY P.start_utc ASC, P.start_time_utc ASC
        ";

        $params = array_merge([(float)$settings['min_confidence']], $enabled_markets);
        $stmt = $pdo->prepare($query);
        $stmt->execute($params);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $total = 0;
        $won = 0;
        $lost = 0;
        $pending = 0;

        $unit_profit = 0;
        $daily_units = [];
        $daily_counts = [];

        foreach ($rows as $row) {
            $res = checkPredictionResult($row['selection'], $row);
            $date = $row['start_utc'];

            if (!isset($daily_units[$date])) {
                $daily_units[$date] = 0;
                $daily_counts[$date] = 0;
            }

            if ($res === 'win') {
                $total++;
                $won++;
                $unit_profit += 1;
                $daily_units[$date] += 1;
                $daily_counts[$date]++;
            } elseif ($res === 'lose') {
                $total++;
                $lost++;
                $unit_profit -= 1;
                $daily_units[$date] -= 1;
                $daily_counts[$date]++;
            } else {
                $pending++;
            }
        }

        ksort($daily_units);

        $labels = [];
        $data = [];
        $running = 0;

        foreach ($daily_units as $date => $unit) {
            $running += $unit;
            $labels[] = date('d.m', strtotime($date));
            $data[] = round($running * $base_unit, 2);
        }

        $hit_rate = ($won + $lost) > 0 ? round(($won / ($won + $lost)) * 100, 2) : null;
        $money_profit = $unit_profit * $base_unit;
        $max_daily_bets = empty($daily_counts) ? 0 : max($daily_counts);

        echo json_encode([
            'status' => 'success',
            'total' => $total,
            'won' => $won,
            'lost' => $lost,
            'pending' => $pending,
            'hit_rate' => $hit_rate,
            'unit_profit' => $unit_profit,
            'money_profit' => $money_profit,
            'labels' => $labels,
            'data' => $data,
            'max_daily_bets' => $max_daily_bets,
        ]);
        exit;
    }

    echo json_encode(['status' => 'error']);
    exit;
}

$settings = getSettings($pdo, $session_id);

$stmt_summary = $pdo->query("
    SELECT
        market_type,
        selection,
        COUNT(*) AS total_predictions,
        AVG(confidence_score) AS avg_confidence,
        SUM(CASE WHEN confidence_label = 'high' THEN 1 ELSE 0 END) AS high_count,
        SUM(CASE WHEN confidence_label = 'medium' THEN 1 ELSE 0 END) AS medium_count
    FROM predictions_football
    GROUP BY market_type, selection
    ORDER BY total_predictions DESC, avg_confidence DESC
");
$market_summary = $stmt_summary->fetchAll(PDO::FETCH_ASSOC);

$short_user_id = lower_tr(substr($session_id, 0, 8)) . '...';

?>
<!DOCTYPE html>
<html lang="tr" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">

    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <meta name="apple-mobile-web-app-title" content="netscout">

    <meta name="mobile-web-app-capable" content="yes">
    <meta name="theme-color" content="#09090b">

    <title>netscout | filtreler</title>

    <script src="https://cdn.tailwindcss.com"></script>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.2/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;900&display=swap" rel="stylesheet">

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: {
                        sans: ['Poppins', 'sans-serif']
                    },
                    colors: {
                        appbg: '#09090b',
                        cardbg: '#18181b'
                    }
                }
            }
        };

        let equityChartInstance = null;

        function toggleGroup(groupId, headerElement) {
            const container = document.getElementById(groupId);
            const icon = headerElement.querySelector('i.fa-chevron-down');

            if (container) container.classList.toggle('hidden');
            if (icon) icon.classList.toggle('-rotate-180');
        }

        async function saveSettings() {
            const form = document.getElementById('settingsForm');
            const button = document.getElementById('saveBtn');

            if (!form || !button) return;

            const originalText = button.innerHTML;
            button.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> kaydediliyor';

            try {
                const formData = new FormData(form);
                formData.append('action', 'save_settings');

                const response = await fetch(window.location.href, {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();

                if (data.status === 'success') {
                    button.innerHTML = '<i class="fa-solid fa-check"></i> kaydedildi';
                    calculateSimulation();

                    setTimeout(() => {
                        button.innerHTML = originalText;
                    }, 1000);
                } else {
                    button.innerHTML = originalText;
                }
            } catch (e) {
                button.innerHTML = originalText;
            }
        }

        async function resetSettings() {
            if (!confirm('tüm filtreleri varsayılana döndürmek istiyor musun?')) return;

            try {
                const formData = new FormData();
                formData.append('action', 'reset_settings');

                const response = await fetch(window.location.href, {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();

                if (data.status === 'success') {
                    window.location.reload();
                }
            } catch (e) {}
        }

        let simTimeout;

        function debounceSimulation() {
            clearTimeout(simTimeout);
            simTimeout = setTimeout(calculateSimulation, 400);
        }

        async function calculateSimulation() {
            const baseInput = document.getElementById('base_unit');
            const baseUnit = baseInput ? parseFloat(baseInput.value || '1000') : 1000;

            try {
                const formData = new FormData();
                formData.append('action', 'calculate_simulation');
                formData.append('base_unit', baseUnit);

                const response = await fetch(window.location.href, {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();

                if (data.status !== 'success') return;

                document.getElementById('sim-total').innerText = data.total;
                document.getElementById('sim-won').innerText = data.won;
                document.getElementById('sim-lost').innerText = data.lost;
                document.getElementById('sim-hit').innerText = data.hit_rate === null ? '-' : '%' + data.hit_rate;
                document.getElementById('sim-max-daily').innerText = data.max_daily_bets;

                const profitEl = document.getElementById('sim-profit');
                const profit = parseFloat(data.money_profit || 0);

                if (profit >= 0) {
                    profitEl.className = 'text-sm font-black text-emerald-400';
                    profitEl.innerText = '+' + profit.toLocaleString('tr-TR', {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2
                    }) + ' ₺';
                } else {
                    profitEl.className = 'text-sm font-black text-red-400';
                    profitEl.innerText = profit.toLocaleString('tr-TR', {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2
                    }) + ' ₺';
                }

                if (equityChartInstance) {
                    equityChartInstance.destroy();
                }

                const canvas = document.getElementById('equityChart');

                if (canvas && data.labels && data.data) {
                    const ctx = canvas.getContext('2d');

                    equityChartInstance = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.labels,
                            datasets: [{
                                data: data.data,
                                borderColor: profit >= 0 ? '#34d399' : '#f87171',
                                backgroundColor: profit >= 0 ? 'rgba(52, 211, 153, 0.10)' : 'rgba(248, 113, 113, 0.10)',
                                borderWidth: 2,
                                pointRadius: 0,
                                pointHoverRadius: 4,
                                fill: true,
                                tension: 0.35
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            animation: {
                                duration: 0
                            },
                            plugins: {
                                legend: {
                                    display: false
                                },
                                tooltip: {
                                    mode: 'index',
                                    intersect: false,
                                    callbacks: {
                                        label: function(context) {
                                            return (context.raw || 0).toLocaleString('tr-TR', {
                                                minimumFractionDigits: 2
                                            }) + ' ₺';
                                        }
                                    }
                                }
                            },
                            scales: {
                                x: {
                                    display: false
                                },
                                y: {
                                    display: false
                                }
                            }
                        }
                    });
                }

            } catch (e) {}
        }

        window.addEventListener('load', () => {
            calculateSimulation();
        });
    </script>

    <style>
        body {
            margin: 0;
            background-color: #09090b;
            color: #f4f4f5;
            -webkit-tap-highlight-color: transparent;
        }

        .hidden {
            display: none !important;
        }

        ::-webkit-scrollbar {
            display: none;
        }
    </style>
</head>

<body class="antialiased min-h-screen pb-10 relative">

<div class="px-[2px] mt-4 sticky top-0 z-50 bg-appbg pt-2 pb-2">
    <div class="flex items-center justify-between gap-1 bg-cardbg p-1 rounded-xl select-none border border-zinc-800/50 shadow-md">

        <a href="index.php" class="w-10 h-8 flex items-center justify-center py-2 text-sm font-bold rounded-lg transition-all text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/30">
            <i class="fa-solid fa-arrow-left w-4 text-center"></i>
        </a>

        <div class="flex-1 flex items-center justify-center gap-2 py-2">
            <i class="fa-solid fa-fingerprint w-4 text-center text-zinc-500 text-sm"></i>
            <span class="text-zinc-300 text-xs font-bold tracking-wider"><?= e($short_user_id) ?></span>
        </div>

        <button onclick="resetSettings()" class="px-3 h-8 bg-zinc-800/30 hover:bg-zinc-800 rounded-lg text-zinc-500 hover:text-zinc-300 transition-colors flex items-center justify-center gap-2 text-xs font-bold">
            <i class="fa-solid fa-rotate-left w-4 text-center"></i>
            <span>sıfırla</span>
        </button>

    </div>
</div>

<main class="px-[2px] mt-4 space-y-8">

    <form id="settingsForm" class="mx-1 space-y-6" onsubmit="event.preventDefault(); saveSettings();">

        <div>
            <div onclick="toggleGroup('filter-container', this)" class="flex justify-between items-center cursor-pointer select-none mb-3 px-2">
                <h2 class="text-xs font-bold text-zinc-300 tracking-wider flex-1 flex items-center">
                    <i class="fa-solid fa-sliders w-4 text-center text-zinc-500 mr-1.5 text-[14px]"></i>
                    tahmin filtreleri
                </h2>

                <div class="w-6 h-6 shrink-0 bg-zinc-800/50 rounded-lg flex items-center justify-center">
                    <i class="fa-solid fa-chevron-down text-zinc-500 text-[10px] transition-transform duration-300"></i>
                </div>
            </div>

            <div id="filter-container" class="bg-cardbg rounded-xl border border-zinc-800/50 p-4 shadow-[0_0_15px_rgba(255,255,255,0.02)] space-y-5">

                <div>
                    <div class="flex items-center justify-between mb-2">
                        <label class="text-xs font-bold text-zinc-300">minimum confidence</label>
                        <span class="text-[11px] text-zinc-500">
                            mevcut: %<?= e(round((float)$settings['min_confidence'] * 100, 1)) ?>
                        </span>
                    </div>

                    <input type="range"
                           name="min_confidence"
                           min="0.50"
                           max="0.95"
                           step="0.01"
                           value="<?= e($settings['min_confidence']) ?>"
                           oninput="document.getElementById('confDisplay').innerText = '%' + Math.round(parseFloat(this.value) * 100)"
                           class="w-full accent-zinc-300">

                    <div class="flex justify-between text-[10px] text-zinc-600 mt-1">
                        <span>%50</span>
                        <span id="confDisplay" class="text-zinc-300 font-bold">%<?= e(round((float)$settings['min_confidence'] * 100)) ?></span>
                        <span>%95</span>
                    </div>

                    <p class="text-[10px] text-zinc-500 mt-2 leading-relaxed">
                        Confidence yükseldikçe daha az ama daha seçici tahmin görünür.
                    </p>
                </div>

                <div class="grid grid-cols-2 gap-2">

                    <?php
                    $market_toggles = [
                        'show_goals' => ['label' => 'gol marketleri', 'icon' => 'fa-futbol'],
                        'show_btts' => ['label' => 'kg marketleri', 'icon' => 'fa-arrows-left-right'],
                        'show_corners' => ['label' => 'korner', 'icon' => 'fa-flag'],
                        'show_shots' => ['label' => 'şut', 'icon' => 'fa-bullseye'],
                        'show_fouls' => ['label' => 'faul', 'icon' => 'fa-hand'],
                        'show_team_goals' => ['label' => 'takım golü', 'icon' => 'fa-crosshairs'],
                    ];

                    foreach ($market_toggles as $name => $meta):
                        $checked = (int)$settings[$name] === 1;
                    ?>

                        <label class="flex items-center justify-between gap-3 bg-zinc-900/60 border border-zinc-800 rounded-xl p-3 cursor-pointer">
                            <span class="flex items-center gap-2 min-w-0">
                                <i class="fa-solid <?= e($meta['icon']) ?> text-zinc-500 w-4 text-center"></i>
                                <span class="text-[11px] font-bold text-zinc-300 truncate"><?= e($meta['label']) ?></span>
                            </span>

                            <input type="checkbox"
                                   name="<?= e($name) ?>"
                                   <?= $checked ? 'checked' : '' ?>
                                   class="accent-zinc-300">
                        </label>

                    <?php endforeach; ?>

                </div>

                <div>
                    <label class="text-xs font-bold text-zinc-300 block mb-2">simülasyon birim tutarı</label>

                    <div class="flex items-center gap-2 bg-zinc-900/60 border border-zinc-800 rounded-xl p-2">
                        <input type="text"
                               inputmode="numeric"
                               pattern="[0-9]*"
                               id="base_unit"
                               name="base_unit"
                               value="<?= e((float)$settings['base_unit']) ?>"
                               oninput="this.value = this.value.replace(/[^0-9]/g, ''); debounceSimulation();"
                               class="flex-1 bg-transparent text-zinc-200 text-sm px-2 focus:outline-none">

                        <span class="text-xs text-zinc-500 pr-2">₺</span>
                    </div>

                    <p class="text-[10px] text-zinc-500 mt-2 leading-relaxed">
                        Oran kullanılmadığı için simülasyon +1 / -1 birim mantığıyla hesaplanır.
                    </p>
                </div>

                <button id="saveBtn" type="submit" class="w-full bg-zinc-200 text-zinc-950 py-3 rounded-xl text-xs font-black flex items-center justify-center gap-2 active:scale-[0.98] transition-all">
                    <i class="fa-solid fa-floppy-disk"></i>
                    ayarları kaydet
                </button>

            </div>
        </div>

    </form>

    <div class="mx-1">
        <div onclick="toggleGroup('simulation-container', this)" class="flex justify-between items-center cursor-pointer select-none mb-3 px-2">
            <h2 class="text-xs font-bold text-zinc-300 tracking-wider flex-1 flex items-center">
                <i class="fa-solid fa-chart-line w-4 text-center text-zinc-500 mr-1.5 text-[14px]"></i>
                isabet simülasyonu
            </h2>

            <div class="w-6 h-6 shrink-0 bg-zinc-800/50 rounded-lg flex items-center justify-center">
                <i class="fa-solid fa-chevron-down text-zinc-500 text-[10px] transition-transform duration-300"></i>
            </div>
        </div>

        <div id="simulation-container" class="bg-cardbg rounded-xl border border-zinc-800/50 p-4 shadow-[0_0_15px_rgba(255,255,255,0.02)]">

            <div class="grid grid-cols-5 gap-2 mb-4">
                <div class="bg-zinc-900/60 rounded-xl p-3 text-center border border-zinc-800">
                    <div class="text-[9px] text-zinc-500 mb-1">toplam</div>
                    <div id="sim-total" class="text-sm font-black text-zinc-200">-</div>
                </div>

                <div class="bg-zinc-900/60 rounded-xl p-3 text-center border border-zinc-800">
                    <div class="text-[9px] text-zinc-500 mb-1">win</div>
                    <div id="sim-won" class="text-sm font-black text-emerald-400">-</div>
                </div>

                <div class="bg-zinc-900/60 rounded-xl p-3 text-center border border-zinc-800">
                    <div class="text-[9px] text-zinc-500 mb-1">lose</div>
                    <div id="sim-lost" class="text-sm font-black text-red-400">-</div>
                </div>

                <div class="bg-zinc-900/60 rounded-xl p-3 text-center border border-zinc-800">
                    <div class="text-[9px] text-zinc-500 mb-1">isabet</div>
                    <div id="sim-hit" class="text-sm font-black text-zinc-200">-</div>
                </div>

                <div class="bg-zinc-900/60 rounded-xl p-3 text-center border border-zinc-800">
                    <div class="text-[9px] text-zinc-500 mb-1">yoğun</div>
                    <div id="sim-max-daily" class="text-sm font-black text-zinc-200">-</div>
                </div>
            </div>

            <div class="bg-zinc-900/60 rounded-xl p-4 border border-zinc-800 mb-4 text-center">
                <div class="text-[10px] text-zinc-500 mb-1">net birim karşılığı</div>
                <div id="sim-profit" class="text-sm font-black text-zinc-200">-</div>
            </div>

            <div class="h-32 w-full relative">
                <canvas id="equityChart"></canvas>
            </div>

            <p class="text-[10px] text-zinc-600 mt-3 leading-relaxed">
                Bu ekran bahis oranı kullanmaz. Her doğru tahmin +1 birim, her yanlış tahmin -1 birim kabul edilir.
            </p>
        </div>
    </div>

    <div class="mx-1">
        <div onclick="toggleGroup('market-summary-container', this)" class="flex justify-between items-center cursor-pointer select-none mb-3 px-2">
            <h2 class="text-xs font-bold text-zinc-300 tracking-wider flex-1 flex items-center">
                <i class="fa-solid fa-layer-group w-4 text-center text-zinc-500 mr-1.5 text-[14px]"></i>
                market özeti
            </h2>

            <div class="w-6 h-6 shrink-0 bg-zinc-800/50 rounded-lg flex items-center justify-center">
                <i class="fa-solid fa-chevron-down text-zinc-500 text-[10px] transition-transform duration-300"></i>
            </div>
        </div>

        <div id="market-summary-container" class="space-y-2">

            <?php if (empty($market_summary)): ?>
                <div class="bg-cardbg rounded-xl border border-zinc-800/50 p-4 text-center text-xs text-zinc-500">
                    henüz tahmin özeti yok.
                </div>
            <?php else: ?>

                <?php foreach ($market_summary as $row): ?>
                    <div class="bg-cardbg rounded-xl border border-zinc-800/50 p-3 flex items-center justify-between gap-3">
                        <div class="min-w-0">
                            <div class="text-xs font-black text-zinc-200 uppercase tracking-wide truncate">
                                <?= e(marketLabel($row['selection'])) ?>
                            </div>

                            <div class="text-[10px] text-zinc-500 mt-1">
                                <?= e(str_replace('_', ' ', $row['market_type'])) ?>
                            </div>
                        </div>

                        <div class="flex items-center gap-2 shrink-0">
                            <div class="text-right">
                                <div class="text-xs font-black text-zinc-300"><?= (int)$row['total_predictions'] ?></div>
                                <div class="text-[9px] text-zinc-600">adet</div>
                            </div>

                            <div class="text-right">
                                <div class="text-xs font-black text-emerald-400">
                                    %<?= e(round((float)$row['avg_confidence'] * 100, 1)) ?>
                                </div>
                                <div class="text-[9px] text-zinc-600">avg</div>
                            </div>
                        </div>
                    </div>
                <?php endforeach; ?>

            <?php endif; ?>

        </div>
    </div>

</main>

</body>
</html>
