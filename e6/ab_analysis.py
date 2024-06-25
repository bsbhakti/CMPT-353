import sys
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def main():
    searchdata_file = sys.argv[1]
    searchdata_file = pd.read_json(f"./{searchdata_file}", orient="records",lines=True)
    # print(searchdata_file)
    boolean_odd_numbers = searchdata_file["uid"] % 2 == 1
    odd_searches = searchdata_file[boolean_odd_numbers]
    even_searches = searchdata_file[~ boolean_odd_numbers] 
    # print(odd_searches,even_searches)
    # plt.hist(odd_searches["search_count"])
    # plt.hist(even_searches["search_count"])
    # plt.show()
    # contingency = pd.crosstab(odd_searches,even_searches)
    searchdata_file["new_search"] = searchdata_file["uid"] %2 == 1
    searchdata_file["searched_atleast_once"] = searchdata_file["search_count"] > 0
    # print(searchdata_file)
    crosstab = pd.crosstab(searchdata_file["new_search"], searchdata_file["searched_atleast_once"])
    # print(crosstab)
    _, pval_chi_all, _, _ = stats.chi2_contingency(crosstab) # categorical data so use chi test. we are tetsing if # of searches is independednt of the search bar
    _, p_utest_all = stats.mannwhitneyu(odd_searches["search_count"],even_searches["search_count"] ) # we are testing if odd numbered people test more than even numbered people so u test
    print("student chi p: ",pval_chi_all) # dont reject the null
    print("student u p: ",p_utest_all) # dont reject null
    instructor_search  =  searchdata_file[(searchdata_file["is_instructor"] == True)]
    instructor_search_odd = odd_searches[odd_searches["is_instructor"] == True]
    instructor_search_even = even_searches[even_searches["is_instructor"] == True]

    # print(instructor_search)
    crosstab_ins = pd.crosstab(instructor_search["new_search"], instructor_search["searched_atleast_once"])
    _, p_chi_ins, _, _ = stats.chi2_contingency(crosstab_ins)
    _, p_utest_ins = stats.mannwhitneyu(instructor_search_odd["search_count"],instructor_search_even["search_count"] ) # we are testing if odd numbered people test more than even numbered people so u test
    print("ins chi p: ",p_chi_ins)  #dont REJECT null
    print("ins u p: ",p_utest_ins) # reject null






    # ...

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=0,
        more_searches_p=0,
        more_instr_p=0,
        more_instr_searches_p=0,
    ))


if __name__ == '__main__':
    main()
