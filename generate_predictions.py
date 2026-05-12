import os
import joblib
import numpy as np
import pandas as pd
import mysql.connector

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, log_loss, classification_report
from sklearn.preprocessing import LabelEncoder


DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
}


MODEL_DIR = "models_markets"
os.makedirs(MODEL_DIR, exist_ok=True)


def connect_db():
    return mysql.connector.connect(**DB_CONFIG)


def load_finished_matches():
    conn = connect_db()

    query = """
    SELECT
        event_id,
        start_utc,
        start_time_utc,
        home_team,
        away_team,
        tournament_id,
        tournament_name,
        country,

        ht_home,
        ht_away,
        ft_home,
        ft_away,

        poss_h,
        poss_a,
        corn_h,
        corn_a,
        shot_h,
        shot_a,
        shot_on_h,
        shot_on_a,
        fouls_h,
        fouls_a,
        offsides_h,
        offsides_a,
        saves_h,
        saves_a,
        passes_h,
        passes_a,
        tackles_h,
        tackles_a
    FROM results_football
    WHERE status IN ('finished', 'ended')
      AND ht_home IS NOT NULL
      AND ht_away IS NOT NULL
      AND ft_home IS NOT NULL
      AND ft_away IS NOT NULL
    ORDER BY start_utc, start_time_utc;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


def add_targets(df):
    df = df.copy()

    df["sh_home"] = df["ft_home"] - df["ht_home"]
    df["sh_away"] = df["ft_away"] - df["ht_away"]

    df["fh_total_goals"] = df["ht_home"] + df["ht_away"]
    df["sh_total_goals"] = df["sh_home"] + df["sh_away"]
    df["total_goals"] = df["ft_home"] + df["ft_away"]

    df["target_fh_btts"] = ((df["ht_home"] > 0) & (df["ht_away"] > 0)).astype(int)
    df["target_sh_btts"] = ((df["sh_home"] > 0) & (df["sh_away"] > 0)).astype(int)

    df["target_home_scores_both_halves"] = ((df["ht_home"] > 0) & (df["sh_home"] > 0)).astype(int)
    df["target_away_scores_both_halves"] = ((df["ht_away"] > 0) & (df["sh_away"] > 0)).astype(int)

    df["target_home_wins_both_halves"] = (
        (df["ht_home"] > df["ht_away"]) &
        (df["sh_home"] > df["sh_away"])
    ).astype(int)

    df["target_away_wins_both_halves"] = (
        (df["ht_away"] > df["ht_home"]) &
        (df["sh_away"] > df["sh_home"])
    ).astype(int)

    df["target_both_halves_over15"] = (
        (df["fh_total_goals"] > 1.5) &
        (df["sh_total_goals"] > 1.5)
    ).astype(int)

    df["target_both_halves_under15"] = (
        (df["fh_total_goals"] < 1.5) &
        (df["sh_total_goals"] < 1.5)
    ).astype(int)

    df["target_goal_range"] = pd.cut(
        df["total_goals"],
        bins=[-1, 1, 3, 5, 99],
        labels=[0, 1, 2, 3]
    ).astype(int)

    df["total_corners"] = df["corn_h"].fillna(0) + df["corn_a"].fillna(0)

    df["target_corners_over85"] = (df["total_corners"] > 8.5).astype(int)
    df["target_corners_over95"] = (df["total_corners"] > 9.5).astype(int)
    df["target_corners_over105"] = (df["total_corners"] > 10.5).astype(int)

    df["target_home_corners_over45"] = (df["corn_h"] > 4.5).astype(int)
    df["target_away_corners_over45"] = (df["corn_a"] > 4.5).astype(int)

    df["target_corner_range"] = pd.cut(
        df["total_corners"],
        bins=[-1, 7, 10, 99],
        labels=[0, 1, 2]
    ).astype(int)

    return df


def make_team_form(df):
    home = df[[
        "event_id", "start_utc", "home_team", "away_team",
        "ft_home", "ft_away", "ht_home", "ht_away",
        "corn_h", "corn_a", "shot_h", "shot_a",
        "shot_on_h", "shot_on_a", "poss_h", "poss_a"
    ]].copy()

    home.columns = [
        "event_id", "start_utc", "team", "opponent",
        "goals_for", "goals_against", "ht_goals_for", "ht_goals_against",
        "corners_for", "corners_against", "shots_for", "shots_against",
        "shots_on_for", "shots_on_against", "poss_for", "poss_against"
    ]

    away = df[[
        "event_id", "start_utc", "away_team", "home_team",
        "ft_away", "ft_home", "ht_away", "ht_home",
        "corn_a", "corn_h", "shot_a", "shot_h",
        "shot_on_a", "shot_on_h", "poss_a", "poss_h"
    ]].copy()

    away.columns = home.columns

    long_df = pd.concat([home, away], ignore_index=True)
    long_df = long_df.sort_values(["team", "start_utc", "event_id"])

    long_df["win"] = (long_df["goals_for"] > long_df["goals_against"]).astype(int)
    long_df["btts"] = ((long_df["goals_for"] > 0) & (long_df["goals_against"] > 0)).astype(int)
    long_df["over25"] = ((long_df["goals_for"] + long_df["goals_against"]) > 2.5).astype(int)

    rolling_cols = [
        "goals_for",
        "goals_against",
        "ht_goals_for",
        "ht_goals_against",
        "corners_for",
        "corners_against",
        "shots_for",
        "shots_against",
        "shots_on_for",
        "shots_on_against",
        "poss_for",
        "win",
        "btts",
        "over25",
    ]

    for col in rolling_cols:
        long_df[f"{col}_last5"] = (
            long_df
            .groupby("team")[col]
            .shift(1)
            .rolling(5, min_periods=2)
            .mean()
            .reset_index(level=0, drop=True)
        )

        long_df[f"{col}_last10"] = (
            long_df
            .groupby("team")[col]
            .shift(1)
            .rolling(10, min_periods=3)
            .mean()
            .reset_index(level=0, drop=True)
        )

    form_cols = [c for c in long_df.columns if c.endswith("_last5") or c.endswith("_last10")]

    return long_df[["event_id", "team"] + form_cols]


def add_form_features(df):
    form = make_team_form(df)

    home_form = form.rename(columns={"team": "home_team"})
    away_form = form.rename(columns={"team": "away_team"})

    home_form = home_form.add_prefix("home_")
    away_form = away_form.add_prefix("away_")

    home_form = home_form.rename(columns={"home_event_id": "event_id"})
    away_form = away_form.rename(columns={"away_event_id": "event_id"})

    df = df.merge(home_form, on="event_id", how="left")
    df = df.merge(away_form, on="event_id", how="left")

    return df


def prepare_features(df):
    df = df.copy()

    df["country"] = df["country"].fillna("unknown")
    df["tournament_name"] = df["tournament_name"].fillna("unknown")

    for col in ["country", "tournament_name"]:
        encoder = LabelEncoder()
        df[col] = encoder.fit_transform(df[col].astype(str))

    base_features = [
        "country",
        "tournament_name",
        "poss_h",
        "poss_a",
        "shot_h",
        "shot_a",
        "shot_on_h",
        "shot_on_a",
        "corn_h",
        "corn_a",
        "fouls_h",
        "fouls_a",
        "passes_h",
        "passes_a",
        "tackles_h",
        "tackles_a",
    ]

    form_features = [
        c for c in df.columns
        if c.startswith("home_") or c.startswith("away_")
    ]

    feature_cols = base_features + form_features

    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df[feature_cols] = df[feature_cols].fillna(df[feature_cols].median(numeric_only=True))

    return df, feature_cols


def train_single_model(df, feature_cols, target_col):
    clean = df.dropna(subset=[target_col]).copy()

    X = clean[feature_cols]
    y = clean[target_col]

    split = int(len(clean) * 0.8)

    X_train = X.iloc[:split]
    X_test = X.iloc[split:]
    y_train = y.iloc[:split]
    y_test = y.iloc[split:]

    model = HistGradientBoostingClassifier(
        max_iter=350,
        learning_rate=0.035,
        max_leaf_nodes=24,
        l2_regularization=0.15,
        random_state=42
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)

    print("\n==============================")
    print("MODEL:", target_col)
    print("Accuracy:", round(accuracy_score(y_test, preds), 4))

    try:
        print("LogLoss:", round(log_loss(y_test, probs), 4))
    except Exception:
        pass

    print(classification_report(y_test, preds))

    joblib.dump(
        {
            "model": model,
            "feature_cols": feature_cols,
            "target_col": target_col,
        },
        f"{MODEL_DIR}/{target_col}.joblib"
    )


def main():
    df = load_finished_matches()
    df = add_targets(df)
    df = add_form_features(df)
    df, feature_cols = prepare_features(df)

    targets = [
        "target_fh_btts",
        "target_sh_btts",
        "target_home_scores_both_halves",
        "target_away_scores_both_halves",
        "target_home_wins_both_halves",
        "target_away_wins_both_halves",
        "target_goal_range",
        "target_both_halves_over15",
        "target_both_halves_under15",
        "target_corners_over85",
        "target_corners_over95",
        "target_corners_over105",
        "target_home_corners_over45",
        "target_away_corners_over45",
        "target_corner_range",
    ]

    for target in targets:
        train_single_model(df, feature_cols, target)


if __name__ == "__main__":
    main()
