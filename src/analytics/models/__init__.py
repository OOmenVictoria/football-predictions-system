"""
Pacchetto per i modelli statistici e predittivi.
Questo pacchetto fornisce implementazioni di diversi modelli per la previsione
di risultati delle partite di calcio e altri eventi correlati.
"""

# Importa i vari modelli
from src.analytics.models.basic_model import BasicModel, create_basic_model
from src.analytics.models.poisson_model import PoissonModel, create_poisson_model
from src.analytics.models.xg_model import XGModel, create_xg_model

def get_model(model_type="basic"):
    """
    Factory per ottenere un'istanza del modello desiderato.
    
    Args:
        model_type: Tipo di modello ("basic", "poisson", "xg")
        
    Returns:
        Istanza del modello richiesto
    """
    if model_type == "basic":
        return create_basic_model()
    elif model_type == "poisson":
        return create_poisson_model()
    elif model_type == "xg":
        return create_xg_model()
    else:
        raise ValueError(f"Tipo di modello non supportato: {model_type}")
