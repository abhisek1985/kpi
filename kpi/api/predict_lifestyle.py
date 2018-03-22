from sklearn.externals import joblib
from ..constants import Constants, ROOT
from six import reraise as raise_
import os
import warnings
from .api_base import dict_wrap

warnings.filterwarnings("ignore")


def predict_lifestyle(property_feature):
    labels = {
        1: 'Beach',
        6: 'Senior',
        4: 'Professional',
        3: 'Family',
        5: 'Rural',
        0: 'Active',
        7: 'Social',
        2: 'City'

    }
    if len(property_feature) != 9:
        if Constants.DEBUG:
            err_msg = 'Property features are not in right shape'
            raise_(ValueError, ValueError(err_msg))
        return False, None
    if property_feature[4] == 'Apartment':
        property_feature.append(1)
        property_feature.append(0)
        property_feature.append(0)
        property_feature.append(0)
    elif property_feature[4] == 'Bungalow':
        property_feature.append(0)
        property_feature.append(1)
        property_feature.append(0)
        property_feature.append(0)
    elif property_feature[4] == 'Cottage':
        property_feature.append(0)
        property_feature.append(0)
        property_feature.append(1)
        property_feature.append(0)
    elif property_feature[4] == 'Villa':
        property_feature.append(0)
        property_feature.append(0)
        property_feature.append(0)
        property_feature.append(1)
    property_feature.pop(4)

    property_id = property_feature[0]

    model = joblib.load(os.path.join(ROOT, 'seller_ml.sav'))
    out = model.predict([property_feature[1::]])
    ret = {
        'property_id': property_id,
        'lifestyle': labels[out[0]]}
    output = lambda: ret
    if Constants.DEBUG:
        import sys
        dwrap = dict_wrap(override=sys.stdout)
        print(dwrap.wrap(functor=output))
        return True, None
    else:
        dwrap = dict_wrap(override=None)
        return True, dwrap.wrap(functor=output)
