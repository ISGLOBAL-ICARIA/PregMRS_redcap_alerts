#!/usr/bin/env python3.8

import alerts
import tokens
import redcap

__author__ = "Andreu Bofill"
__copyright__ = "Copyright 2024, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20240520"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Dev"

if __name__ == '__main__':
    for project_key in tokens.PREGMRS_REDCAP_PROJECTS:
        project = redcap.Project(tokens.URL, tokens.PREGMRS_REDCAP_PROJECTS[project_key])
        alerts.pregmrs_alert(project)

