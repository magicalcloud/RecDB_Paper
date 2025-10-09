# RecDB: An LSM-Tree based Storage System for Training Large Recommendation Model in Low-Resource Scenarios

## install RecDB

```
cd src
python setup.py clean --all
python setup.py build_ext --inplace
```

## dataset
```
Criteo Kaggle: https://kaggle.com/criteo-display-ad-challenge
Criteo Terabyte: https://ailab.criteo.com/downloadcriteo-1tb-click-logs-dataset/
```

## experiment results
To facilitate plotting the experimental graphs for the paper, We have placed the results in the "experiment_results" directory. We can be easily executed by running：
```
cd experiment_results
python exp[exp_id].py
```
