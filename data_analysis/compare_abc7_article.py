# compare the numbers in the abc 7 article to our numbers
# https://abc7chicago.com/cta-bus-tracker-app-estimated-arrival-times-chicago/13995684/
import pandas as pd
import data_analysis.compare_scheduled_and_rt as csrt

abc7_dict = {
    'num_cancelled_trips': 360000,
    'num_cancelled_trips_aug_2022': 27477,
    'num_cancelled_trips_aug_2023': 4937
}

def compare_numbers(combined_long_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    """Compare the numbers in the abc7 article with our data

    Args:
        combined_long_df (pd.DataFrame): first part of tuple output
            from data_analysis.compare_scheduled_and_rt.main.
            Example.

                    date        route_id  trip_count_rt  ...
            0  2022-05-20        1             43
            1  2022-05-20      100             33
            2  2022-05-20      103             80
            3  2022-05-20      106            121
            4  2022-05-20      108             34

        summary_df (pd.DataFrame): second part of tuple output from 
            data_analysis.compare_scheduled_and_rt.main
            Example.

            route_id day_type  trip_count_rt  trip_count_sched     ratio
            0        1      hol             47                59  0.796610
            1        1       wk           9872             11281  0.875100
            2      100      hol             38                53  0.716981
            3      100       wk           9281             10177  0.911958
            4      103      hol            475               552  0.860507
    """
    num_cancelled_trips_ours = summary_df['trip_count_sched'].sum() - summary_df['trip_count_rt'].sum()    
    print(f'Article number of cancelled trips {abc7_dict["num_cancelled_trips"]}'
          f'\nOur number of cancelled trips: {num_cancelled_trips_ours}')
    combined_long_df['date'] = pd.to_datetime(combined_long_df['date'])
    aug_dict = {}
    for year in [2022, 2023]:
        aug_dict[f'combined_long_df_aug_{year}'] = combined_long_df.loc[
            (combined_long_df['date'].dt.month == 8)
            & (combined_long_df['date'].dt.year == year)
        ]
        aug_dict[f'num_cancelled_trips_aug_{year}_ours'] = (
            aug_dict[f'combined_long_df_aug_{year}']['trip_count_sched'].sum()
            - aug_dict[f'combined_long_df_aug_{year}']['trip_count_rt'].sum()
        )
        print(f'\nArticle number cancelled trips in August {year}: ' f'{abc7_dict[f"num_cancelled_trips_aug_{year}"]}')
        print(f'Our number of cancelled trips in August {year}: ' f'{aug_dict[f"num_cancelled_trips_aug_{year}_ours"]}')
     
    # Test routes in article
    summary_df['cancelled_trips'] = summary_df['trip_count_sched'] - summary_df['trip_count_rt']
    route_list = ['49', '49X', '22', '9', '9X', '63']
    rankings = summary_df.groupby('route_id').sum()['cancelled_trips'].rank(ascending=False)
    for route_id in route_list:
        route_id_ranking = rankings[rankings.index == route_id]
        if route_id_ranking.empty:
            print(f'\nroute {route_id} does not exist in our data')
            continue
        print(f'\nroute {route_id} is ranked {route_id_ranking[0]} for number of canceled trips')


def main() -> None:
    combined_long_df, summary_df = csrt.combine_real_time_rt_comparison()
    compare_numbers(summary_df, combined_long_df)


if __name__ == '__main__':
    main()
