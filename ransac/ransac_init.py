import numpy as np
import random


from sklearn import datasets


class LinearExample:

    def initSamplesForRansac(self):
        n_samples = 1000
        n_outliers = 50

        X, y, coef = datasets.make_regression(n_samples=n_samples, n_features=1,
                                          n_informative=1, noise=10,
                                          coef=True, random_state=0)

        # Add outlier data
        np.random.seed(0)
        X[:n_outliers] =  2 * np.random.normal(size=(n_outliers, 1))
        y[:n_outliers] =  10 * np.random.normal(size=n_outliers)

        samples = []
        for i in range(0,len(X)-1):
            samples.append({'x': X[i][0], 'y': y[i]})
        return samples

    def createModelFromSamples(self,samples):

        dx = 0
        while ( dx == 0):
            creator_samples = []
            for i in range(0,2):
                index = random.randint(0, len(samples)-1);
                x = samples[index]['x']
                y = samples[index]['y']
                creator_samples.append({'x': x, \
                                        'y': y} )
                # print("creator_samples ",i, " : ", creator_samples, " index ", index)
            dx = creator_samples[0]['x'] - creator_samples[1]['x']

        # model = <a,b> where y = ax+b
        # so given x1,y1 and x2,y2 =>
        #  y1 = a*x1 + b
        #  y2 = a*x2 + b
        #  y1-y2 = a*(x1 - x2) ==>  a = (y1-y2)/(x1-x2)
        #  b = y1 - a*x1

        return self.modelFromSamplePair(creator_samples[0], creator_samples[1])

    @staticmethod
    def modelFromSamplePair(sample1, sample2):
        dx = sample1['x'] - sample2['x']

        if(dx == 0):
            dx = 0.0001

        # model = <a,b> where y = ax+b
        # so given x1,y1 and x2,y2 =>
        #  y1 = a*x1 + b
        #  y2 = a*x2 + b
        #  y1-y2 = a*(x1 - x2) ==>  a = (y1-y2)/(x1-x2)
        #  b = y1 - a*x1

        a = (sample1['y'] - sample2['y']) / dx
        b = sample1['y'] - sample1['x'] * a
        return {'a': a, 'b': b}

    def scoreModelAgainstSamples(self, model, samples):
        # predict the y using the model and x samples, per sample, and sum the abs of the distances between the real y
        # with truncation of the error at distance CUTOFF_DIST

        def scoreModelAgainstSingleSample(model, sample):
            CUTOFF_DIST = 20;
            pred_y = model['a'] * sample['x'] + model['b']
            error = min(abs(sample['y'] - pred_y),CUTOFF_DIST)
            return error

        totalScore = 0
        for sample_i in range(0,len(samples)-1):
            sample = samples[sample_i]
            score = scoreModelAgainstSingleSample(model, sample)
            totalScore += score

        # print("model ",model, " score ", totalScore)
        return totalScore


def ransac(samples, iterations, modelFromSamplesFunc, scoreModelFunc ):
    # runs ransac algorithm for the given amount of iterations, where in each iteration it:
    # 1. randomly creates a model from the samples by calling m = modelFromSamplesFunc(samples)
    # 2. calculating the score of the model against the sample set
    # 3. keeps the model with the best score
    # after all iterations are done - returns the best model and score

    min_m = {}
    min_score = -1;
    for i in range(1,iterations):
        m = modelFromSamplesFunc(samples)
        score = scoreModelFunc(m, samples)

        if(min_score < 0 or score < min_score):
            min_score = score
            min_m = m

    return {'model': min_m, 'score': min_score }

def ransacSpark(samples, iterations, modelFromSamplesFunc, scoreModelFunc, sc ):
    # runs ransac algorithm for the given amount of iterations, where in each iteration it:
    # 1. randomly creates a model from the samples by calling m = modelFromSamplesFunc(samples)
    # 2. calculating the score of the model against the sample set
    # 3. keeps the model with the best score
    # after all iterations are done - returns the best model and score


    # make samples into an RDD

    base_rdd = sc.parallelize(samples)
    # get #iterations randomly sampled samples - as the 1st of pair of creator samples
    creatorSamples_left = sc.parallelize(base_rdd.takeSample(False, iterations))

    # get #iterations randomly sampled samples - as the 2nd of pair of creator samples
    creatorSamples_right = sc.parallelize(base_rdd.takeSample(False, iterations))

    # combine the random samples into pairs
    creatorSamples_pairs = creatorSamples_left.zip(creatorSamples_right)

    # now map to create a model for each pair ==> rdd of models
    models = creatorSamples_pairs.map(lambda pair : modelFromSamplesFunc(pair[0],pair[1]))

    # calculate scores for all models again
    modelsAndScores = models.map(lambda m : { 'model': m, 'score': scoreModelFunc(m,samples)})

    # find the minimal score model
    minModel = modelsAndScores.reduce( lambda a, b : a if(a['score']<b['score']) else b )

    return minModel