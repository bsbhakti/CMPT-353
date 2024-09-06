import sys
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from datetime import date


OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]
    counts = pd.read_json(reddit_counts, lines=True)
    counts = counts.loc[(counts['date']>= "01-01-2012") & (counts['date'] <= "31-12-2013")].copy()
    counts["subreddit"] = counts["subreddit"].str.lower()
    counts = counts.loc[counts["subreddit"] == "canada"].copy()
    weekend_counts = counts.loc[(counts["date"].dt.dayofweek == 5) | (counts["date"].dt.dayofweek == 6)].copy()
    weekday_counts = counts.loc[(counts["date"].dt.dayofweek != 5) & (counts["date"].dt.dayofweek != 6)].copy()


    weekday_stat_fail, weekend_stat_fail = stats.normaltest(weekday_counts["comment_count"]),stats.normaltest(weekend_counts["comment_count"])
    var_fail = stats.levene(weekday_counts["comment_count"],weekend_counts["comment_count"])
    ttest_fail = stats.ttest_ind(weekday_counts["comment_count"], weekend_counts["comment_count"],alternative="two-sided")
    # print("First ttest fail:\n",ttest_fail,"\n")

    weekday_counts["comment_count_transformed"] = np.sqrt(weekday_counts["comment_count"])
    weekend_counts["comment_count_transformed"] = np.sqrt( weekend_counts["comment_count"]  ) 

    # print("Transformed normality test:\n")
    weekday_stat_transformed, weekend_stat_transformed = stats.normaltest(weekday_counts["comment_count_transformed"]),stats.normaltest(weekend_counts["comment_count_transformed"])
    var_transformed = stats.levene(weekday_counts["comment_count_transformed"],weekend_counts["comment_count_transformed"])
    # print(f"{weekday_stat_transformed} \n {weekend_stat_transformed} \n{var_transformed}\n")
    # fig, axs = plt.subplots(2)
    # axs[0].hist(weekday_counts["comment_count_transformed"])
    # axs[1].hist(weekend_counts["comment_count_transformed"])
    # plt.show()

  
    weekend_counts_cpy = weekend_counts.copy()
    weekday_counts_cpy = weekday_counts.copy()

    time_weekend =  ( (weekend_counts["date"]).dt.isocalendar()  ) 
    time_weekday =  ( (weekday_counts["date"]).dt.isocalendar()  ) 
    weekend_counts = weekend_counts.join(time_weekend)
    weekday_counts = weekday_counts.join(time_weekday)
    weekend_counts = weekend_counts.groupby(by=["year","week"],as_index=False).mean("comment_count")
    weekday_counts = weekday_counts.groupby(by=["year","week"],as_index=False).mean("comment_count")
    print(weekday_counts)
    # weekend_counts = weekend_counts.loc[(weekend_counts["year"] >=2012) & (weekend_counts["year"] <= 2013)]
    # weekday_counts = weekday_counts.loc[(weekday_counts["year"] >=2012) & (weekday_counts["year"] <= 2013)]
    weekend_counts = weekend_counts[["year","week","comment_count"]]
    weekday_counts = weekday_counts[["year","week","comment_count"]]


    # print("Second normality test after grouping and averaging:\n")
    weekend_counts_stat = stats.normaltest(weekend_counts["comment_count"])
    weekday_counts_stat = stats.normaltest(weekday_counts["comment_count"])
    var = stats.levene(weekday_counts["comment_count"],weekend_counts["comment_count"])
    # print(f"{weekend_counts_stat.pvalue}\n,{weekday_counts_stat.pvalue}\n , {var}\n")
    # fig, axs = plt.subplots(2)
    # axs[0].hist(weekday_counts["comment_count"])
    # axs[1].hist(weekend_counts["comment_count"])
    # plt.show()
    # print("Second ttest after grouping:\n")
    ttest = stats.ttest_ind(weekday_counts["comment_count"], weekend_counts["comment_count"])
    # print(ttest)

    # print("Man whitney test:\n")
    whitney = stats.mannwhitneyu(weekday_counts_cpy["comment_count"], weekend_counts_cpy["comment_count"],alternative='two-sided')
    # print(whitney)










    
    # ...

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=ttest_fail.pvalue,
        initial_weekday_normality_p=weekday_stat_fail.pvalue,
        initial_weekend_normality_p=weekend_stat_fail.pvalue,
        initial_levene_p=var_fail.pvalue,
        transformed_weekday_normality_p=weekday_stat_transformed.pvalue,
        transformed_weekend_normality_p=weekend_stat_transformed.pvalue,
        transformed_levene_p=var_transformed.pvalue,
        weekly_weekday_normality_p=weekday_counts_stat.pvalue,
        weekly_weekend_normality_p=weekend_counts_stat.pvalue,
        weekly_levene_p=var.pvalue,
        weekly_ttest_p=ttest.pvalue,
        utest_p=whitney.pvalue,
    ))


if __name__ == '__main__':
    main()
