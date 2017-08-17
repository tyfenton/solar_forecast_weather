import pandas as pd
from pvlib.forecast import GFS, NAM, NDFD, HRRR, RAP
import datetime
import os


def forecast_download(model_name, latitude, longitude, start, end, meta):
    # HRRR is the most accurate data model provides for 24 hours at 1 hour increments
    # RAP is the parent model of HRRR, also provides for 24 hours at 1 hour increments
    # NAM model provides 1 hour data for 48 hours then 3 hour data for the following 48 hours
    # NDFD model provides 1 hour data for 48 hours then 3 hour data for the following 24 hours
    # GFS model provides 3 hour data for 7 days. GFS is the most established and popular model
    weather_models = {'HRRR': HRRR, 'RAP': RAP, 'NAM': NAM, 'NDFD': NDFD, 'GFS': GFS}
    weather_model = weather_models[model_name]()
    weather_data = weather_model.get_processed_data(latitude, longitude, start, end)
    weather_hourly = weather_data.resample('H').interpolate()
    weather_hourly = weather_hourly[:-1]  # remove last row that is 00:00 on the 8th day
    print(meta['Name'], model_name, 'download complete')
    return weather_hourly


def forecast(meta, past_forecast, day, model):
    site_name = meta['Name']
    # Download the weather data using pvlib functions
    latitude = meta['latitude']
    longitude = meta['longitude']
    tz = meta['tz']
    # set variables used to pull in weather forecasts
    # shift 4 hours so forecast starts at midnight (Hardcoded for US/Easter)
    start = pd.Timestamp(datetime.date.today() + pd.Timedelta(days=day), tz=tz) - pd.Timedelta(hours=4)
    end = (start + pd.Timedelta(days=1))
    new_forecast = forecast_download(model, latitude, longitude, start, end, meta)
    total_forecast = pd.concat([past_forecast, new_forecast])
    # rename columns (optional)
    total_forecast.index.name = 'Date'
    total_forecast.columns = [site_name + ': Tamb', site_name + ': Wind_Speed', site_name + ': GHI',
                              site_name + ': DNI', site_name + ': DHI', site_name + ': Total_Clouds',
                              site_name + ': Low_Clouds', site_name + ': Mid_Clouds', site_name + ': High_Clouds']
    # some frustratingly illogical timestamp work to get the datetime to line up correctly without timestamps
    total_forecast.index = total_forecast.index.tz_convert('Etc/GMT-4').tz_convert(None)
    total_forecast.index = total_forecast.index.tz_localize('Etc/GMT-4').tz_convert(None)

    return total_forecast


def main():
    config_filename = "<file path>"
    forecast_dir = "<folder path>"
    # sites is a file with rows of sites names and columns of site location specs
    sites = pd.read_csv(config_filename, encoding='latin-1', skiprows=[1], comment='#')
    # Create list of sites to be evaluated
    site_names = list(set(sites['Plant Name'].tolist()))
    site_names.sort()
    for n in site_names:
        n_lower = n.lower()
        site_meta = {'latitude': sites[sites['Plant Name'] == n].iloc[0]['Latitude'],
                     'longitude': sites[sites['Plant Name'] == n].iloc[0]['Longitude'],
                     'tz': sites[sites['Plant Name'] == n].iloc[0]['Timezone'], 'Name': n}
        # create dict with the days ahead to forecast and the model type.
        forecast_list = {'day_1_hrrr': 'HRRR', 'day_1_rap': 'RAP', 'day_1_gfs': 'GFS', 'day_2_gfs': 'GFS'}
        for fname, model in forecast_list.items():
            day_int = int(fname.split('_')[1]) - 1
            if not os.path.isfile(forecast_dir + n_lower + '_' + fname + '_historical_forecasts.csv'):
                past_forecast = pd.DataFrame()
                site_forecast = forecast(site_meta, past_forecast, day_int, model)
            else:
                past_forecast = pd.read_csv(forecast_dir + n_lower + '_' + fname + '_' '_historical_forecasts.csv')
                site_forecast = forecast(site_meta, past_forecast, day_int, model)
            site_forecast.to_csv(forecast_dir + n_lower + '_' + fname + '_' '_historical_forecasts.csv')


if __name__ == '__main__':
    main()