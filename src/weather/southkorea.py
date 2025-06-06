import os
import time
import itertools
import requests

## Constants
DATA_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

## Variables
branch_id_name = {
    90: "Sokcho",
    93: "Bukchuncheon",
    95: "Cheorwon",
    98: "Dongducheon",
    99: "Paju",
    100: "Daegwallyeong",
    101: "Chuncheon",
    102: "Baengnyeongdo",
    104: "Bukgangneung",
    105: "Gangneung",
    106: "Donghae",
    108: "Seoul",
    112: "Incheon",
    114: "Wonju",
    115: "Ulleungdo",
    119: "Suwon",
    121: "Yeongwol",
    127: "Chungju",
    129: "Seosan",
    130: "Uljin",
    131: "Cheongju",
    133: "Daejeon",
    135: "Chupungnyeong",
    136: "Andong",
    137: "Sangju",
    138: "Pohang",
    140: "Gunsan",
    143: "Daegu",
    146: "Jeonju",
    152: "Ulsan",
    155: "Changwon",
    156: "Gwangju",
    159: "Busan",
    162: "Tongyeong",
    165: "Mokpo",
    168: "Yeosu",
    169: "Heuksando",
    170: "Wando",
    172: "Gochang",
    174: "Suncheon",
    177: "Hongseong",
    184: "Jeju",
    185: "Gosan",
    188: "Seongsan",
    189: "Seogwipo",
    192: "Jinju",
    201: "Ganghwa",
    202: "Yangpyeong",
    203: "Icheon",
    211: "Inje",
    212: "Hongcheon",
    216: "Taebaek",
    217: "Jeongseon",
    221: "Jecheon",
    226: "Boeun",
    232: "Cheonan",
    235: "Boryeong",
    236: "Buyeo",
    238: "Geumsan",
    239: "Sejong",
    243: "Buan",
    244: "Imsil",
    245: "Jeongeup",
    247: "Namwon",
    248: "Jangsu",
    251: "Gochang",
    252: "Yeonggwang",
    253: "Gimhae",
    254: "Sunchang",
    255: "Bukchangwon",
    257: "Yangsan",
    258: "Boseong",
    259: "Gangjin",
    260: "Jangheung",
    261: "Haenam",
    262: "Goheung",
    263: "Uiryeong",
    264: "Hamyang",
    266: "Gwangyang",
    268: "Jindo",
    271: "Bonghwa",
    272: "Yeongju",
    273: "Mungyeong",
    276: "Cheongsong",
    277: "Yeongdeok",
    278: "Uiseong",
    279: "Gumi",
    281: "Yeongcheon",
    283: "Gyeongju",
    284: "Geochang",
    285: "Hapcheon",
    288: "Miryang",
    289: "Sancheong",
    294: "Geoje",
    295: "Namhae",
}

wind_direction_code_name = {
    0: "N",
    20: "NNE",
    50: "NE",
    70: "ENE",
    90: "E",
    110: "ESE",
    140: "SE",
    160: "SSE",
    180: "S",
    200: "SSW",
    230: "SW",
    250: "WSW",
    270: "W",
    290: "WNW",
    320: "NW",
    340: "NNW",
    360: "N",
}

## Functions
def get_request_params(data_key: str, branch_id:str, date: str, hour: str) -> dict:
    '''Get request parameters'''
    request_params = {
        "serviceKey": data_key,
        "stnIds": branch_id,
        "startDt": date,
        "endDt": date,
        "startHh": hour.zfill(2),
        "endHh": hour.zfill(2),
        "pageNo": "1",
        "numOfRows": "30",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR"
    }
    return request_params

def get_object_name(directory: str, date: str, hour: str) -> str:
    '''Get object name'''
    return directory + "/" + date[0:4] + "/" + date[4:6] + "/" + date[6:8] + "/" + hour.zfill(2) + ".parquet"

def convert_string_int(string: str) -> int:
    '''Convert type string to int'''
    if string == "":
        return 0
    return int(string)

def convert_string_float(string: str) -> float:
    '''Convert type string to float'''
    if string == "":
        return 0.0
    return float(string)

def convert_wd_code_name(code: str) -> str:
    '''Convert wind direction code to name'''
    if code == "":
        return "NONE"
    return wind_direction_code_name[int(code)]

def get_southkorea_weather_data(api_key: str, request_date: str, request_hour: str) -> dict:
    # Get data
    branch_id_response = {}
    for branch_id in branch_id_name:
        # Request data and retry if failed
        for i in itertools.count(start=0):
            response = requests.get(
                DATA_URL,
                params=get_request_params(api_key, branch_id, request_date, request_hour),
                timeout=600,
            )
            print("--- Branch ID : {0} / Status Code : {1} ---".format(branch_id, response.status_code))
            print(response.headers)
            print(response.content)

            # Check response
            if (response.status_code == 200 and
                response.headers.get("content-type") == "application/json;charset=UTF-8" and
                response.json()["response"]["header"]["resultCode"] == "00"):
                branch_id_response[branch_id] = response.json()
                break

            # Retry 5 times with sleep 5 seconds
            if i == 5:
                raise ValueError("failed to get data")
            else:
                print("retry : {0}".format(i + 1))
                time.sleep(5)

    # Init merged dict
    merged_hourly_data = {
        "branch_name": [],
        "temp": [],
        "rain": [],
        "snow": [],
        "cloud_cover_total": [],
        "cloud_cover_lowmiddle": [],
        "cloud_lowest": [],
        "cloud_shape": [],
        "humidity": [],
        "wind_speed": [],
        "wind_direction": [],
        "pressure_local": [],
        "pressure_sea": [],
        "pressure_vaper": [],
        "dew_point": [],
    }

    # Parsing and merge data
    for branch_id, branch_name in branch_id_name.items():
        # Get hourly data
        hourly_data = branch_id_response[branch_id]["response"]["body"]["items"]["item"][0]

        # Parsing data
        temp = convert_string_float(hourly_data["ta"])  # °C
        rain = convert_string_float(hourly_data["rn"])  # mm
        snow = convert_string_float(hourly_data["dsnw"])  # cm
        cloud_cover_total = convert_string_int(hourly_data["dc10Tca"])  # null (1 ~ 10)
        cloud_cover_lowmiddle = convert_string_int(hourly_data["dc10LmcsCa"])  # null (1 ~ 10)
        cloud_lowest = convert_string_int(hourly_data["lcsCh"])  # 100m
        cloud_shape = hourly_data["clfmAbbrCd"]  # null (Cloud Shape Abbreviation)
        humidity = convert_string_int(hourly_data["hm"])  # %
        wind_speed = convert_string_float(hourly_data["ws"])  # m/s
        wind_direction = convert_wd_code_name(hourly_data["wd"])  # null (0 ~ 360)
        pressure_local = convert_string_float(hourly_data["pa"])  # hpa
        pressure_sea = convert_string_float(hourly_data["ps"])  # hpa
        pressure_vaper = convert_string_float(hourly_data["pv"])  # hpa
        dew_point = convert_string_float(hourly_data["td"])  # °C

        # Merge data
        merged_hourly_data["branch_name"].append(branch_name)
        merged_hourly_data["temp"].append(temp)
        merged_hourly_data["rain"].append(rain)
        merged_hourly_data["snow"].append(snow)
        merged_hourly_data["cloud_cover_total"].append(cloud_cover_total)
        merged_hourly_data["cloud_cover_lowmiddle"].append(cloud_cover_lowmiddle)
        merged_hourly_data["cloud_lowest"].append(cloud_lowest)
        merged_hourly_data["cloud_shape"].append(cloud_shape)
        merged_hourly_data["humidity"].append(humidity)
        merged_hourly_data["wind_speed"].append(wind_speed)
        merged_hourly_data["wind_direction"].append(wind_direction)
        merged_hourly_data["pressure_local"].append(pressure_local)
        merged_hourly_data["pressure_sea"].append(pressure_sea)
        merged_hourly_data["pressure_vaper"].append(pressure_vaper)
        merged_hourly_data["dew_point"].append(dew_point)

    # Show dataframe
    print("--- Merged Data ---")
    print(merged_hourly_data)

    return merged_hourly_data