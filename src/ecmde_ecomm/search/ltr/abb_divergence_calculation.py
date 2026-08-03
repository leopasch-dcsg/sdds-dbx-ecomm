from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from ecmde_ecomm.common.dbx.elastic.divergence_calculation import (
    DivergenceOperation,
    DivergenceConfig,
)
from scipy.special import betaln,digamma
import pandas as pd
import numpy as np
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from scipy.stats import beta


class ABBDivergenceCalculation(DivergenceOperation):
    def __init__(
            self,
            config: DivergenceConfig,
            spark: SparkSession,
    ):
        super().__init__(config, spark)

    @staticmethod
    @F.pandas_udf(returnType=DoubleType())
    def signed_kl_divergence_beta(
            alpha1: pd.Series,
            beta1: pd.Series,
            alpha2: pd.Series,
            beta2: pd.Series,
    ) -> pd.Series:
        """
        Computes the KL divergence D_KL(prior || posterior) between two Beta distributions:
        prior ~ Beta(alpha1, beta1)
        posterior ~ Beta(alpha2, beta2)

        If the posterior is shifted to the right of the prior, the KL divergence is positive.

        KL divergence formula between two beta distributions taken from here:
        https://math.stackexchange.com/questions/257821/kullback-liebler-divergence/1018520#1018520
        """
        first_term = betaln(alpha2, beta2) - betaln(alpha1, beta1)
        second_term = (alpha1 - alpha2) * digamma(alpha1)
        third_term = (beta1 - beta2) * digamma(beta1)
        fourth_term = (alpha2 - alpha1 + beta2 - beta1) * digamma(alpha1 + beta1)

        kl_div = first_term + second_term + third_term + fourth_term

        # comparing means to figure out the sign
        prior_mean = alpha1 / (alpha1 + beta1)
        posterior_mean = alpha2 / (alpha2 + beta2)

        # if posterior's mean is higher than (or equal to) the priors, the sign should be positive

        # note about means being equal:  The prior stddev will always be lower than the posterior's stddev due to the way the prior is calculated (ie with global and query-level data). The posterior is calculated with query-ecode level clicks and impressions which will, by definition by less than (or possibly equal to) query level clicks and impressions and thus the standard dev of the posterior won't be greater than the prior's.  I made the decision that, if the means are equal (and the divergence is non-zero), the divergence should be positive (even though we're more certain of the prior's mean value).

        signs = np.where(posterior_mean >= prior_mean, 1, -1)

        return signs * kl_div

    @staticmethod
    @F.pandas_udf(returnType=DoubleType())
    def signed_js_divergence(
            alpha1: pd.Series,
            beta1: pd.Series,
            alpha2: pd.Series,
            beta2: pd.Series,
            kl_div: pd.Series,
    ) -> pd.Series:
        """
        Computes the JS divergence D_JS(prior || posterior) between two Beta distributions:
        prior ~ Beta(alpha1, beta1)
        posterior ~ Beta(alpha2, beta2)

        If the posterior is shifted to the right of the prior, the JS divergence is positive.
        """
        batch_size = len(alpha1)
        num_samples = 1000

        beta_fcn_1 = beta(alpha1, beta1)
        beta_fcn_2 = beta(alpha2, beta2)

        def mean_log_density_ratio(p_dist, q_pdf):
            epsilon = 1e-10
            cap = 1e10
            samples = np.clip(p_dist.rvs(size=(num_samples, batch_size)), epsilon, cap)
            prob_p = np.clip(p_dist.pdf(samples), epsilon, cap)
            prob_q = np.clip(q_pdf(samples), epsilon, cap)
            log_ratio = np.log(prob_p) - np.log(prob_q)
            return np.sum(log_ratio, axis=0) / num_samples

        kl_beta1_to_mixture = mean_log_density_ratio(
            beta_fcn_1,
            lambda x: (beta_fcn_1.pdf(x) + beta_fcn_2.pdf(x)) / 2,
        )

        kl_beta2_to_mixture = mean_log_density_ratio(
            beta_fcn_2,
            lambda x: (beta_fcn_1.pdf(x) + beta_fcn_2.pdf(x)) / 2,
        )

        js_div = (kl_beta1_to_mixture + kl_beta2_to_mixture) / 2
        signs = np.where(kl_div > 0, 1, -1)
        signed_js_div = signs * js_div
        return pd.Series(signed_js_div + np.log(2))

    def batch_dataframe(self) -> DataFrame:
        table_name = self.config.destination_table_qualified()
        dataframe = self.spark.read.table(table_name)

        dataframe = dataframe.withColumn(
            "signed_kl_divergence",
            self.__class__.signed_kl_divergence_beta(
                'prior_alpha',
                'prior_beta',
                'alpha_n',
                'beta_n'
            )
        )

        dataframe = dataframe.withColumn(
            "signed_js_divergence_shifted",
            self.__class__.signed_js_divergence(
                'prior_alpha',
                'prior_beta',
                'alpha_n',
                'beta_n',
                'signed_kl_divergence'
            )
        )

        return dataframe