import pandas as pd
import matplotlib.pyplot as plot
from scipy.stats import linregress
import seaborn as sns
def to_timestamp(date):
    return date.timestamp()



dogRatesTweets = pd.read_csv("./dog_rates_tweets.csv", index_col=0)
dogRatesTweets['created_at'] = pd.to_datetime(dogRatesTweets['created_at'],format="mixed",utc=True)

pattern = r'(\d+(\.\d+)?)/10'
rating = dogRatesTweets['text'].str.contains(pattern,regex=True)
dogRatesTweets_filt = dogRatesTweets[rating]
del dogRatesTweets
rating = dogRatesTweets_filt['text'].str.extract(pattern)[0].astype(float)
dogRatesTweets_filt.insert(2,"rating",rating,True) ##1675 here
dogRatesTweets_filt = dogRatesTweets_filt[dogRatesTweets_filt["rating"] < 20]
# dogRatesTweets_filt ##1669 here


dogRatesTweets_filt['timestamp'] = dogRatesTweets_filt['created_at'].apply(to_timestamp)
# dogRatesTweets_filt['timestamp'] = to_timestamp(dogRatesTweets_filt['created_at'])
fit1 = linregress(dogRatesTweets_filt["timestamp"], dogRatesTweets_filt["rating"])
pvalue = fit1.pvalue
print(fit1.slope)
fit1 =  dogRatesTweets_filt['timestamp']*fit1.slope + fit1.intercept
dogRatesTweets_filt['prediction'] = fit1
dogRatesTweets_filt["residual"] = dogRatesTweets_filt["rating"] - dogRatesTweets_filt["prediction"]
plot.hist(dogRatesTweets_filt["residual"])
plot.show()
del fit1
pvalue

plot.xticks(rotation=25)
plot.plot(dogRatesTweets_filt["created_at"],dogRatesTweets_filt["rating"], 'b.', alpha=0.5)
plot.plot(dogRatesTweets_filt["created_at"], dogRatesTweets_filt['prediction'] ,'r-', linewidth=3)
plot.xlabel("Year")
plot.ylabel("Rating")
plot.title("Rating by Year")
plot.show()

dogRatesTweets_filt['year'] = dogRatesTweets_filt['created_at'].dt.year
dogRatesTweets_filt['month'] = dogRatesTweets_filt['created_at'].dt.month



avg_prediction_per_year = dogRatesTweets_filt.groupby(['year', 'month'])['rating'].mean().reset_index()
print(avg_prediction_per_year.head(5))


# Rename columns for clarity
avg_prediction_per_year.columns = ['year', 'month', 'avg_rating']

plot.figure(figsize=(12, 6))

# Set the style
sns.set(style="whitegrid")

# Loop through each year to plot a line
for year in avg_prediction_per_year['year'].unique():
    # Filter data for the specific year
    yearly_data = avg_prediction_per_year[avg_prediction_per_year['year'] == year]
    # Plot the line for that year
    plot.plot(yearly_data['month'], yearly_data['avg_rating'], marker='o', label=f'Year {year}')

# Customize the plot
plot.title('Monthly Average Rating by Year')
plot.xlabel('Month')
plot.ylabel('Average Rating')
plot.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plot.legend(title='Year')
plot.grid(True)

# Show the plot
plot.tight_layout()
plot.show()

# Display the result
print(avg_prediction_per_year)
