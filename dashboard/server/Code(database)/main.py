from configparser import ConfigParser
import argparse, sys, os

from database import *
from fsl_lambda_analysis import *
from x_ref_item_lambda_analysis import *
from qc_a_vs_k_docs import *
from pivot_table import *
from q1abc import *
from q2abc import *
from cash_flow_vs_disruption_idx import *
from module1_actual_data import *
from module1_2_regression_setup import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('excel_args', nargs='*', help='Enter excel path')
    args = parser.parse_args()
    excel_args = args.excel_args
    def invalid_usage(msg):
        sys.stderr.write('ERROR: %s%s' % (msg, os.linesep))
        parser.print_usage(sys.stderr)
        sys.exit(1)

    # Ensure enough args are specified
    if len(excel_args) != 1:
        invalid_usage('Insufficient args. Please supply exactly 1.')
    path_to_excel = excel_args[0]
    read_from_excel(path_to_excel)
    fsl_lambda_main()
    xref_main()
    qc_main()
    pivot_main()
    q1_main()
    q2_main()
    cash_disr_main()
    module1_main()
    module_1_2_regression_main()
