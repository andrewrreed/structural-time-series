import os
import cdsw

def fit_models_parallel():
    '''
    Use the CDSW Workers API (via Python SDK) to launch each model fitting script in parallel

    Docs - https://docs.cloudera.com/machine-learning/cloud/distributed-computing/topics/ml-workers-api.html

    '''
    base_path = os.getcwd()
    script_path = base_path + '/scripts'

    scripts = os.listdir(script_path)
    scripts = [script_path+'/'+script for script in scripts if script[0:3]=='fit']

    for script in scripts:
        cdsw.launch_workers(n=1, cpu=1, memory=2, script=script)


if __name__ == "__main__":
    fit_models_parallel()
