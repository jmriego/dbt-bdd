from steps.utils import hash_value

import sys
import os
import io

BEHAVE_DEBUG_ON_ERROR = False
DBT_UNIT_TEST_SEEDS_PATH = 'data/unit-test'
DBT_TARGET = 'dev'
DBT_PROFILES_DIR = '~/.dbt'

def setup_debug_on_error(userdata):
    global BEHAVE_DEBUG_ON_ERROR
    BEHAVE_DEBUG_ON_ERROR = userdata.getbool("BEHAVE_DEBUG_ON_ERROR")

def before_all(context):
    setup_debug_on_error(context.config.userdata)
    context.seeds_path = DBT_UNIT_TEST_SEEDS_PATH
    try:
        os.mkdir(context.seeds_path)
    except FileExistsError:
        pass
    context.target = DBT_TARGET
    context.profiles_dir = DBT_PROFILES_DIR
    # context.real_stdout = sys.stdout
    # context.stdout_mock = io.StringIO()
    # context.exit_mock = Mock()
    # sys.stdout = context.stdout_mock
    # sys.exit = context.exit_mock

def after_scenario(context, scenario):
# #     sys.stdout = context.real_stdout
    if hasattr(context, 'dbt_rpc'):
        context.dbt_rpc.exit()

def before_scenario(context, scenario):
    feature = scenario.feature
    context.feature_id = hash_value(feature.name)
    context.scenario_name = f'{feature.name} || {scenario.name}'
    context.scenario_id = hash_value(context.scenario_name)
    context.seeds = []
    context.seed_templates = {}

def before_step(context, step):
    context.step_name = f'{context.scenario_name} || {step.name}'
    context.step_id = hash_value(context.step_name)

def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == "failed":
        import pdb; pdb.post_mortem(step.exc_traceback)
