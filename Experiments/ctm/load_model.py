import os
import pickle


def load_model(
    # model_dir="../Experiments/saved_models/ctm_gpu",
    model_dir="Experiments\saved_models\ctm_gpu",
    model_name: str = "ctm_model_10topics_20250707_0250",
):
    model_path = os.path.join(model_dir, f"{model_name}.pkl")
    if not os.path.exists(model_path):
        return None
    with open(model_path, "rb") as f:
        return pickle.load(f)


def get_latest_model_name(model_dir: str = "Experiments\saved_models\ctm_gpu"):
    """
    Get the name of the latest LDA model in the model directory.

    Parameters:
    - model_dir: Directory where models are stored

    Returns:
    - Name of the latest model, or None if no models found
    """
    if not os.path.exists(model_dir):
        return None

    # Look for .pkl files
    model_files = [f for f in os.listdir(model_dir)]
    if not model_files:
        return None

    # Sort by modification time (newest first)
    model_files.sort(
        key=lambda x: os.path.getmtime(os.path.join(model_dir, x)), reverse=True
    )

    # Get the base name (remove .model extension)
    latest_model = model_files[0].replace(".pkl", "")
    return latest_model
