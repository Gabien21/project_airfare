import pandas as pd
import numpy as np
import os
import joblib
import logging
from unidecode import unidecode
from sklearn.model_selection import train_test_split, KFold, GridSearchCV
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    root_mean_squared_error
)

from src.utils.logger_utils import setup_logger

# ========================== Directory Setup ==========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_DIR = os.path.join(BASE_DIR, "data")
MODEL_DIR = os.path.join(BASE_DIR, "models")

# ========================== Model Selection Class ==========================
class ModelSelectorCV:
    """Perform K-Fold cross-validation to compare different models."""
    def __init__(self, models, X_train, y_train, X_val=None, y_val=None, X_test=None, y_test=None, n_splits=5):
        self.models = models
        self.X_train = X_train
        self.y_train = y_train
        self.X_val = X_val
        self.y_val = y_val
        self.X_test = X_test
        self.y_test = y_test
        self.n_splits = n_splits
        self.results = []

    def cross_validate_model(self, model, name):
        """Evaluate a model using K-Fold CV and return metrics."""
        logging.info(f"Cross-validating model: {name}")
        kf = KFold(n_splits=self.n_splits, shuffle=True, random_state=42)
        r2_scores = []
        rmse_scores = []

        for train_idx, val_idx in kf.split(self.X_train):
            X_tr, X_val = self.X_train.iloc[train_idx], self.X_train.iloc[val_idx]
            y_tr, y_val = self.y_train.iloc[train_idx], self.y_train.iloc[val_idx]

            model.fit(X_tr, y_tr)
            y_pred = model.predict(X_val)
            r2_scores.append(r2_score(y_val, y_pred))
            rmse_scores.append(root_mean_squared_error(y_val, y_pred))

        return {
            "model_name": name,
            "mean_r2": np.mean(r2_scores),
            "mean_rmse": np.mean(rmse_scores),
            "model_object": model
        }

    def run_cv(self):
        """Run CV for all models and print their average scores."""
        logging.info(f"Running K-Fold CV with {self.n_splits} folds...")
        for name, model in self.models:
            try:
                result = self.cross_validate_model(model, name)
                self.results.append(result)
                logging.info(f"{name:15} | R2: {result['mean_r2']:.4f} | RMSE: {result['mean_rmse']:.2f}")
            except Exception as e:
                logging.error(f"Error evaluating {name}: {e}")

    def get_best_model(self, by="mean_r2"):
        """Return best model based on R2 or RMSE."""
        if by == "mean_r2":
            return sorted(self.results, key=lambda x: (x["mean_r2"], -x["mean_rmse"]), reverse=True)[0]
        elif by == "mean_rmse":
            return sorted(self.results, key=lambda x: (x["mean_rmse"], -x["mean_r2"]))[0]
        else:
            raise ValueError("Only support by 'mean_r2' or 'mean_rmse'.")

# ========================== Model Training Workflow ==========================
def split_into_train_val_test(X, y):
    """Split dataset into train, validation and test sets."""
    logging.info("Splitting data into train, val, and test sets...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.1, random_state=42, shuffle=True)
    return X_train, X_val, X_test, y_train, y_val, y_test

def modelSelction(X_train, y_train, n_split=5):
    """Define and evaluate a list of candidate models using CV."""
    models = [
        ("LinearRegression", LinearRegression()),
        ("Ridge", Ridge()),
        ("RandomForest", RandomForestRegressor(n_estimators=100, random_state=42)),
        ("XGBoost", XGBRegressor(n_estimators=100, random_state=42)),
        ("LightGBM", LGBMRegressor(n_estimators=100, random_state=42)),
    ]
    selector = ModelSelectorCV(models, X_train, y_train, n_splits=n_split)
    selector.run_cv()
    return selector

# def model_finetuning(model, X_train, y_train, X_val, y_val):
#     """Use GridSearchCV to tune hyperparameters of the selected model."""
#     logging.info("🔧 Performing hyperparameter tuning with GridSearchCV...")
#     param_grid = {
#         'n_estimators': [100, 200, 300],
#         'learning_rate': [0.01, 0.1],
#         'max_depth': [3, 6, 10],
#         'subsample': [0.7, 1.0],
#         'colsample_bytree': [0.7, 1.0]
#     }

#     grid_search = GridSearchCV(
#         estimator=model,
#         param_grid=param_grid,
#         scoring='r2',
#         cv=5,
#         verbose=1,
#         n_jobs=-1
#     )
#     grid_search.fit(X_train, y_train)

#     best_model = grid_search.best_estimator_
#     y_pred_eval = best_model.predict(X_val)

#     logging.info(f"Best hyperparameters: {grid_search.best_params_}")
#     logging.info(f"Eval R²: {r2_score(y_val, y_pred_eval):.4f}")
#     logging.info(f"Eval RMSE: {root_mean_squared_error(y_val, y_pred_eval):.2f}")
#     return best_model

def model_finetuning(model, X_train, y_train, X_val, y_val, model_name=""):
    """Perform GridSearchCV tuning based on model type."""
    logging.info(f"Starting hyperparameter tuning for {model_name}...")

    # Define parameter grid for each model
    param_grids = {
        "XGBoost": {
            'n_estimators': [100, 200],
            'learning_rate': [0.01, 0.1],
            'max_depth': [3, 6],
            'subsample': [0.7, 1.0],
            'colsample_bytree': [0.7, 1.0]
        },
        "LightGBM": {
            'n_estimators': [100, 200],
            'learning_rate': [0.01, 0.1],
            'max_depth': [3, 6, -1],
            'subsample': [0.7, 1.0],
            'colsample_bytree': [0.7, 1.0]
        },
        "RandomForest": {
            'n_estimators': [100, 200],
            'max_depth': [None, 10, 20],
            'max_features': ['sqrt', 'log2']
        },
        "Ridge": {
            'alpha': [0.1, 1.0, 10.0],
            'fit_intercept': [True, False]
        }
        # LinearRegression does not need tuning
    }

    if model_name not in param_grids:
        logging.warning(f"No tuning grid defined for model '{model_name}'. Skipping GridSearch.")
        model.fit(X_train, y_train)
        return model

    # Perform Grid Search
    param_grid = param_grids[model_name]
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        scoring='r2',
        cv=5,
        verbose=1,
        n_jobs=-1
    )
    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_
    y_pred_eval = best_model.predict(X_val)

    logging.info(f"Best hyperparameters for {model_name}: {grid_search.best_params_}")
    logging.info(f"Eval R²: {r2_score(y_val, y_pred_eval):.4f}")
    logging.info(f"Eval RMSE: {root_mean_squared_error(y_val, y_pred_eval):.2f}")
    return best_model


def model_final_training_and_testing(model, scaler, X_train, y_train, X_test, y_test):
    """Train final model on full train set and evaluate on test set."""
    logging.info("Final training and evaluation on test set...")
    model.fit(X_train, y_train)
    y_pred_test = model.predict(X_test)

    logging.info("TEST RESULTS (scaled):")
    logging.info(f"R²: {r2_score(y_test, y_pred_test):.4f}")
    logging.info(f"RMSE: {root_mean_squared_error(y_test, y_pred_test):.2f}")
    logging.info(f"MAE: {mean_absolute_error(y_test, y_pred_test):.2f}")

    # Inverse transform to original scale
    y_test_original = scaler['Total_Price'].inverse_transform(y_test.values.reshape(-1, 1))
    y_pred_original = scaler['Total_Price'].inverse_transform(y_pred_test.reshape(-1, 1))

    logging.info("TEST RESULTS (original scale):")
    logging.info(f"R²: {r2_score(y_test_original, y_pred_original):.4f}")
    logging.info(f"RMSE: {np.sqrt(mean_squared_error(y_test_original, y_pred_original)):.2f}")
    logging.info(f"MAE: {mean_absolute_error(y_test_original, y_pred_original):.2f}")

    return model

# ========================== Data Loading ==========================
def load_data():
    """Load feature dataset and preprocessing artifacts."""
    logging.info("Loading training data and preprocessing artifacts...")
    df = pd.read_csv(os.path.join(DATA_DIR, "data_for_modeling", "data.csv"))
    scaler = joblib.load(os.path.join(MODEL_DIR, "scalers_per_column.pkl"))
    label_binarizer = joblib.load(os.path.join(MODEL_DIR, "multilabel_binarizer_refund_policy.pkl"))
    return df, scaler, label_binarizer

# ========================== Main Pipeline ==========================
def model_data():
    """Main function to run model selection, tuning, and evaluation."""
    setup_logger(log_dir="logs")
    logging.info("Starting model training pipeline...")
    df, scaler, label_binarizer = load_data()
    X = df.drop(columns=['Total_Price'])
    y = df['Total_Price']
    X.columns = [unidecode(c).strip("- ").strip().replace(" ", "_").replace(",", "") for c in X.columns]

    X_train, X_val, X_test, y_train, y_val, y_test = split_into_train_val_test(X, y)
    selector = modelSelction(X_train, y_train)

    best_model = selector.get_best_model()["model_object"]
    model_name = selector.get_best_model()['model_name']
    logging.info(f"Best model from CV: {model_name}")

    best_model = model_finetuning(best_model, X_train, y_train, X_val, y_val, model_name=model_name)
    best_model = model_final_training_and_testing(best_model, scaler, pd.concat([X_train, X_val]), pd.concat([y_train, y_val]), X_test, y_test)

    joblib.dump(best_model, os.path.join(MODEL_DIR, "final_best_model.pkl"))
    logging.info("Final model saved successfully.")

if __name__ == "__main__":
    model_data()
