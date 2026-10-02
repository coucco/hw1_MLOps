import pandas as pd
import logging
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

model = CatBoostClassifier()
model.load_model('./models/my_catboost.cbm')

model_th = 0.98
logger.info('Pretrained model imported successfully...')

def make_pred(dt, source_info="kafka"):

    expected_categorical = ['hour',
                            'year',
                            'month',
                            'day_of_month',
                            'day_of_week',
                            'gender_cat',
                            'merch_cat',
                            'cat_id_cat',
                            'one_city_cat',
                            'us_state_cat',
                            'jobs_cat']
    for col in expected_categorical:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    submission = pd.DataFrame({
        'score':  model.predict_proba(dt)[:, 1],
        'fraud_flag': (model.predict_proba(dt)[:, 1] > model_th) * 1
    })
    logger.info(f'Prediction complete for data from {source_info}')

    return submission
