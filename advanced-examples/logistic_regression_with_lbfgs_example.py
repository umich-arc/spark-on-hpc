#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Logistic Regression With LBFGS Example.
"""
from __future__ import print_function

import sys
from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="PythonLogisticRegressionWithLBFGSExample")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        values = [float(x) for x in line.split(' ')]
        return LabeledPoint(values[0], values[1:])

    input_svm_text_file = str(sys.argv[1]) if len(sys.argv) > 1 else "/data/sample_svm_data.txt"
    data = sc.textFile(input_svm_text_file)
    parsedData = data.map(parsePoint)

    # Build the model
    model = LogisticRegressionWithLBFGS.train(parsedData)

    # Evaluating the model on training data
    labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda lp: lp[0] != lp[1]).count() / float(parsedData.count())
    print("############################################################################")
    print("MLlib works. Logistic Regression complete.")
    print("Training Error = " + str(trainErr))
    print("############################################################################")

    # Save and load model
    model.save(sc, "target/tmp/pythonLogisticRegressionWithLBFGSModel")
    sameModel = LogisticRegressionModel.load(sc,
                                             "target/tmp/pythonLogisticRegressionWithLBFGSModel")
    # $example off$
    sc.stop()
