import pandas as pd
import os
import argparse
import sys
import ast

def get_sample(df, type_of_sample, no_of_sample):
  if type_of_sample == 'fraction':
    return df.sample(frac = no_of_sample)
  else:
    return df.sample(n = int(no_of_sample))

def get_tweets_on_dates(df, all_dates, type_of_sample='fraction', no_of_sample=1):
  
  temp_df = pd.DataFrame()
  for a_date in all_dates:
    if ':' in a_date:
      start_date, end_date = a_date.split(':')
      temp_df = pd.concat([temp_df, get_sample(df[df.created_at.between(start_date, end_date)], type_of_sample, no_of_sample)])
    else:
      temp_df = pd.concat([temp_df, get_sample(df[df['created_at'].str.contains(a_date)], type_of_sample, no_of_sample)])
  
  return temp_df


def get_tweets_on_keywords(df, keywords, type_of_sample='frac', no_of_sample=1):
  
  temp_df = pd.DataFrame()
  for keyword in keywords:
    temp_df = pd.concat([temp_df, get_sample(df[df['created_at'].str.contains(keyword)], type_of_sample, no_of_sample)])
  
  return temp_df

parser = argparse.ArgumentParser(description='Sample tweets for the AfriHate project before annotation.')
parser.add_argument('-i', '--input', required=True, type=str, help='csv/tsv file containing the tweets.')
parser.add_argument('-o', '--output', default='', type=str, help='path to save the sampled tweets.')
parser.add_argument('-t', '--sample_type', default='fraction', choices=['fraction', 'number'], help='type of tweets sampling (fraction of tweets or a number of tweets).')
parser.add_argument('-n', '--no_of_samples', required=True, type=float, help='number of tweets to return per sample.')
parser.add_argument('--sample_stopwords', action='store_true', help='use if tweets are sampled based on stopwords.')
parser.add_argument('--sample_periods', action='store_true', help='use if tweets are sampled based on interesting time periods.')
parser.add_argument('-s', '--stopwords', type=str, default='', help="stopwords file. required if '--sample_stopwords' option is used.")
parser.add_argument('-p', '--periods', type=str, default='', help="comma-seperated time periods for tweet sampling. supported formats include single dates such as YY, YY-MM and YY-MM-DD o range such as YY-MM-DD:YY-MM-DD or YY-MM:YY-MM or YY:YY. required if '--sample_periods' option is used.")

args = parser.parse_args()

if args.sample_type == 'fraction':
  assert args.no_of_samples > 0 and args.no_of_samples <= 1, 'sample fraction must be between 0 and 1'
if args.sample_stopwords:
  assert args.stopwords != '', 'sample_stopwords is used without providing stopwords file'
if args.sample_periods:
  assert args.periods != '', 'sample_periods is used without providing time periods'

if not (args.sample_stopwords and args.sample_periods):
  print('no sampling method is provided. please provide at least one of sampling based on stopwords or time periods')
  sys.exit(-1)

if args.sample_stopwords:
  try:
    with open(args.stopwords, 'r') as f:
      stopwords = f.read().splitlines()
  except:
    print('cannot read stopwords file. check and correct the file or file path')
    sys.exit(-1)

if args.sample_periods:
  periods = ast.literal_eval(args.periods)
  assert len(periods) != 0, 'empty time periods are provided'

tweets_file = args.input

if tweets_file.endswith('.csv'):
  df = pd.read_csv(args.input, header = 0)
elif tweets_file.endswith('.tsv'):
  df = pd.read_csv(args.input, sep='\t', header = 0)
else:
  print('tweets file format not supported, please provide a csv/tsv file')
  sys.exit(-1)

assert 'created_at' in list(df.columns), 'created_at (required) is not a column in the tweets file'
assert 'text' in list(df.columns), 'text (required) is not a column in the tweets file'

if args.sample_type == 'number':
  assert args.no_of_samples <= len(df), 'sample fraction must be between 0 and 1'

if not os.path.isdir(args.output):
  os.makedirs(args.output)

print('number of tweets:', len(df))
if args.sample_type == 'fraction':
  print('number of tweets to be sampled:', str(args.no_of_samples * 100) + '%' , 'of all tweets')
else:
  print('number of tweets to be sampled:', args.no_of_samples , 'tweets')

sampled_df = pd.concat([get_tweets_on_dates(df, periods, args.sample_type, args.no_of_samples), 
                        get_tweets_on_keywords(df, keywords, args.sample_type, args.no_of_samples)])

output_file = os.path.join(args.output, 'sampled_tweets.csv')

sampled_df.to_csv(output_file, index = False)
print('saved', len(sampled_df), 'sampled tweets in', output_file)