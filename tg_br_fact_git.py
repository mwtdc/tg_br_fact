#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import io
import logging
import os
import pathlib
import urllib
import urllib.parse
import warnings
from sys import platform
from time import sleep

import numpy as np
import pandas as pd
import pymysql
import pyodbc
import requests
import skimage.io
import skimage.util
import yaml
from PIL import Image
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

start_time = datetime.datetime.now()
warnings.filterwarnings("ignore")


# Настройки для логера
logging.basicConfig(
    filename=str(pathlib.Path(__file__).parent.absolute())
    + "log_journal_tg_br_fact.log",
    level=logging.INFO,
    format=(
        "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s"
    ),
)


# Загружаем yaml файл с настройками
with open(
    str(pathlib.Path(__file__).parent.absolute()) + "/settings.yaml",
    "r",
    encoding="utf-8",
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings["telegram"])
sql_settings = pd.DataFrame(settings["sql_db"])
pyodbc_settings = pd.DataFrame(settings["pyodbc_db"])
ses = pd.DataFrame(settings["ses"])
grafana_settings = pd.DataFrame(settings["grafana"])

#  Задаем переменные путей (если папка есть, то чистим, если нет, то создаем)
graf_screen = str(pathlib.Path(__file__).parent.absolute()) + "/grafana/"
if not os.path.exists(graf_screen):
    os.makedirs(graf_screen)
list(
    map(
        os.unlink,
        (os.path.join(graf_screen, f) for f in os.listdir(graf_screen)),
    )
)
parent_path = str(pathlib.Path(__file__).parent.absolute())
gecko_path = str(pathlib.Path(__file__).parent.absolute()) + "/geckodriver.exe"
firefox_path = (
    str(pathlib.Path(__file__).parent.absolute())
    + "/FirefoxPortable/App/Firefox64/firefox.exe"
)


#  Настройки для драйвера Firefox (скрытый режим и установка драйвера(закоменчена),
# берется geckodriver.exe из этой же папки и portable версия firefox (чтобы работало даже на чистой системе)
options = Options()
options.headless = True  # True - скрытый режим, False - показывая браузер.
# options.headless = False # True - скрытый режим, False - показывая браузер.
options.binary_location = firefox_path
options.set_preference("network.http.phishy-userpass-length", 255)
serv = Service(gecko_path)
# browser = webdriver.Firefox(options=options, executable_path=GeckoDriverManager().install())
browser = webdriver.Firefox(options=options, service=serv)


# Раздел с функциями


# Функция отправки уведомлений в telegram на любое количество каналов (указать данные в yaml файле настроек)
def telegram(i, text, media_path, media_caption):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])

    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False
    s.post(
        "https://api.telegram.org/bot"
        + bot_token
        + "/sendMessage?chat_id="
        + channel_id
        + "&text="
        + msg
    )
    if media_path != None:
        s.post(
            "https://api.telegram.org/bot"
            + bot_token
            + "/sendPhoto?chat_id="
            + channel_id,
            data={"caption": media_caption},
            files={"photo": open(media_path, "rb")},
        )


# Функция загрузки ПБР.(для коннекта к нужной базе задать порядковый номер числом !!! начинается с 0 !!!!!)
def pbr_load(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    connection_vc = pymysql.connect(
        host=host_yaml,
        user=user_yaml,
        port=port_yaml,
        password=password_yaml,
        database=database_yaml,
    )
    conn_cursor = connection_vc.cursor()
    sql = (
        "SELECT GTP_ID,GTP_NAME,dt,TG,PmaxPDG,PmaxBR,TotalBR,OCPU,OCPS FROM"
        " pbr_br_grafana.pbr_br WHERE dt = CURDATE() - INTERVAL 1 HOUR AND"
        " SESSION_NUMBER IN (SELECT MAX(SESSION_NUMBER) FROM"
        " pbr_br_grafana.pbr_br WHERE dt = CURDATE()- INTERVAL 1 HOUR)UNION"
        " SELECT GTP_ID,GTP_NAME,dt,TG,PmaxPDG,PmaxBR,TotalBR,OCPU,OCPS FROM"
        " pbr_br_grafana.pbr_br WHERE dt >= CURDATE() AND SESSION_NUMBER IN"
        " (SELECT MAX(SESSION_NUMBER) FROM pbr_br_grafana.pbr_br WHERE dt ="
        " CURDATE()) ORDER BY GTP_ID,dt;"
    )
    conn_cursor.execute(sql)
    br_dataframe_orig = pd.DataFrame(
        conn_cursor.fetchall(),
        columns=[
            "GTP_ID",
            "GTP_NAME",
            "dt",
            "TG",
            "PmaxPDG",
            "PmaxBR",
            "TotalBR",
            "OCPU",
            "OCPS",
        ],
    )

    br_dataframe_now = br_dataframe_orig.drop(
        br_dataframe_orig.index[
            np.where(
                br_dataframe_orig["dt"]
                != (
                    datetime.datetime.now().replace(
                        microsecond=0, second=0, minute=0
                    )
                    + datetime.timedelta(hours=-1)
                )
            )[0]
        ]
    )
    br_dataframe_now = pd.DataFrame(
        np.array(br_dataframe_now),
        columns=[
            "GTP_ID",
            "GTP_NAME",
            "dt",
            "TG",
            "PmaxPDG",
            "PmaxBR",
            "TotalBR",
            "OCPU",
            "OCPS",
        ],
    )
    br_dataframe_prev = br_dataframe_orig.drop(
        br_dataframe_orig.index[
            np.where(
                br_dataframe_orig["dt"]
                != (
                    datetime.datetime.now().replace(
                        microsecond=0, second=0, minute=0
                    )
                    + datetime.timedelta(hours=-2)
                )
            )[0]
        ]
    )
    br_dataframe_prev = pd.DataFrame(
        np.array(br_dataframe_prev),
        columns=[
            "GTP_ID",
            "GTP_NAME",
            "dt",
            "TG",
            "PmaxPDG",
            "PmaxBR",
            "TotalBR",
            "OCPU",
            "OCPS",
        ],
    )
    br_dataframe = br_dataframe_now.drop(["TotalBR", "TG"], axis=1)
    br_dataframe["TotalBR"] = (
        br_dataframe_now["TotalBR"] + br_dataframe_prev["TotalBR"]
    ) / 2
    br_dataframe["TG"] = (br_dataframe_now["TG"] + br_dataframe_prev["TG"]) / 2
    connection_vc.commit()
    connection_vc.close()
    return br_dataframe


# Функция загрузки факта выработки.(для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)
def fact_load(i):
    server = str(pyodbc_settings.host[i])
    database = str(pyodbc_settings.database[i])
    username = str(pyodbc_settings.user[i])
    password = str(pyodbc_settings.password[i])
    # Выбор драйвера в зависимости от ОС
    if platform == "linux" or platform == "linux2":
        connection_ms = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + server
            + ";DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )
    elif platform == "win32":
        connection_ms = pyodbc.connect(
            "DRIVER={SQL Server};SERVER="
            + server
            + ";DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )

    mssql_cursor = connection_ms.cursor()
    mssql_cursor.execute(
        "SELECT SUBSTRING (Points.PointName ,len(Points.PointName)-8, 8) as"
        " gtp, MIN(DT) as DT, SUM(Val) as Val FROM Points JOIN PointParams ON"
        " Points.ID_Point=PointParams.ID_Point JOIN PointMains ON"
        " PointParams.ID_PP=PointMains.ID_PP WHERE PointName like"
        " 'Генерация%{GVIE%' AND ID_Param=153 AND DT >= DATEADD(HOUR, -1,"
        " DATEDIFF(d, 0, GETDATE())) AND PointName NOT LIKE '%GVIE0001%' AND"
        " PointName NOT LIKE '%GVIE0012%' AND PointName NOT LIKE '%GVIE0416%'"
        " AND PointName NOT LIKE '%GVIE0167%' AND PointName NOT LIKE"
        " '%GVIE0264%' AND PointName NOT LIKE '%GVIE0007%' AND PointName NOT"
        " LIKE '%GVIE0680%' AND PointName NOT LIKE '%GVIE0987%' AND PointName"
        " NOT LIKE '%GVIE0988%' AND PointName NOT LIKE '%GVIE0989%' AND"
        " PointName NOT LIKE '%GVIE0991%' AND PointName NOT LIKE '%GVIE0994%'"
        " GROUP BY SUBSTRING (Points.PointName ,len(Points.PointName)-8, 8),"
        " DATEPART(YEAR, DT), DATEPART(MONTH, DT), DATEPART(DAY, DT),"
        " DATEPART(HOUR, DT) ORDER BY SUBSTRING (Points.PointName"
        " ,len(Points.PointName)-8, 8), DATEPART(YEAR, DT), DATEPART(MONTH,"
        " DT), DATEPART(DAY, DT), DATEPART(HOUR, DT);"
    )
    fact = mssql_cursor.fetchall()
    connection_ms.close()
    fact = pd.DataFrame(np.array(fact), columns=["gtp", "dt", "fact"])
    fact.drop_duplicates(
        subset=["gtp", "dt"], keep="last", inplace=True, ignore_index=False
    )
    # Удаляем все кроме последнего часа
    fact.drop(
        fact.index[
            np.where(
                fact["dt"]
                != (
                    datetime.datetime.now().replace(
                        microsecond=0, second=0, minute=0
                    )
                    + datetime.timedelta(hours=-1)
                )
            )[0]
        ],
        inplace=True,
    )
    return fact


# Функция запуска Grafana
def grafana_load():
    browser.get(
        "http://xxx.xxx.xx.xxx:xxxx/d/Monitoring_SPP/monitoring-spp?orgId=1&refresh=15m"
    )
    browser.set_window_size(1920, 3240)
    # browser.fullscreen_window()
    try:
        browser.find_element("name", "user").send_keys(
            str(grafana_settings.user[0])
        )
        browser.find_element("name", "password").send_keys(
            str(grafana_settings.password[0])
        )
        browser.find_element(
            "css selector", '[aria-label="Login button"]'
        ).click()
    except:
        pass
    finally:
        sleep(30)
    return True


# Функция получения скрина из Grafana
def grafana_screenshot(GTP_NAME):
    featureElement = browser.find_element(
        "css selector", '[aria-label="' + GTP_NAME + ' panel"]'
    ).screenshot_as_png
    imageStream = io.BytesIO(featureElement)
    im = Image.open(imageStream)
    im = im.resize((299, 182), Image.ANTIALIAS)
    im.save(graf_screen + GTP_NAME + ".png")


# Функция получения списка файлов из папки
def getfilenames(filepath=graf_screen, filelist_out=[], file_ext="all"):
    for fpath, dirs, fs in os.walk(filepath):
        for f in fs:
            fi_d = os.path.join(fpath, f)
            if file_ext == "all":
                filelist_out.append(fi_d)
            elif os.path.splitext(fi_d)[1] == file_ext:
                filelist_out.append(fi_d)
            else:
                pass
    return filelist_out


id_message = str(int(datetime.datetime.now().timestamp()))
telegram(
    1, "Старт мониторинга отклонений графиков \n ID: " + id_message, None, None
)

# Загрузка факта выработки
logging.info("Старт. Загрузка факта выработки.")
fact_dataframe = fact_load(0)
logging.info("Факт выработки загружен")

# Загрузка ПБР
logging.info("Старт. Загрузка ПБР.")
br_dataframe = pbr_load(0)
logging.info("ПБР загружен")

# Склейка датафреймов факта и ПБР
logging.info("Старт. Подготовка итогового датафрейма.")
br_dataframe.drop("GTP_NAME", axis="columns", inplace=True)
br_dataframe = br_dataframe.merge(
    ses, left_on=["GTP_ID"], right_on=["GTP_ID"], how="left"
)
compare_dataframe = br_dataframe.merge(
    fact_dataframe,
    left_on=["GTP_ID", "dt"],
    right_on=["gtp", "dt"],
    how="left",
)
compare_dataframe.fillna(0, inplace=True)
compare_dataframe["fact"] = compare_dataframe["fact"] / 1000
compare_dataframe.sort_values(by=["GTP_NAME"], inplace=True, ignore_index=True)
logging.info("Финиш. Подготовка итогового датафрейма.")

# Анализ отклонений
grafana_load()

datetime_header = (
    str(
        datetime.datetime.now().replace(microsecond=0, second=0, minute=0)
        + datetime.timedelta(hours=0)
    )
    + "\n"
)
message_str = datetime_header
# Дозагрузка
text_up = "⚡️Дозагрузка:\n"
for row_index in range(len(compare_dataframe.index)):
    if compare_dataframe.TG[row_index] < compare_dataframe.TotalBR[row_index]:
        diff = round(
            compare_dataframe.TotalBR[row_index]
            - compare_dataframe.TG[row_index],
            3,
        )
        if compare_dataframe.OCPU[row_index] > 0:
            text_up = (
                text_up
                + str(compare_dataframe.GTP_NAME[row_index])
                + " на "
                + str(diff)
                + " МВт⬆️(ОЦПУ)\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
        elif compare_dataframe.OCPU[row_index] == 0:
            text_up = (
                text_up
                + str(compare_dataframe.GTP_NAME[row_index])
                + " на "
                + str(diff)
                + " МВт⬆️\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
if text_up != "⚡️Дозагрузка:\n":
    message_str = message_str + text_up

# Разгрузка
text_down = "⚡️Разгрузка:\n"
for row_index in range(len(compare_dataframe.index)):
    if compare_dataframe.TG[row_index] > compare_dataframe.TotalBR[row_index]:
        diff = round(
            compare_dataframe.TG[row_index]
            - compare_dataframe.TotalBR[row_index],
            3,
        )
        if compare_dataframe.OCPS[row_index] > 0:
            text_down = (
                text_down
                + str(compare_dataframe.GTP_NAME[row_index])
                + " на "
                + str(diff)
                + " МВт🔻(ОЦПС)\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
        elif compare_dataframe.OCPS[row_index] == 0:
            text_down = (
                text_down
                + str(compare_dataframe.GTP_NAME[row_index])
                + " на "
                + str(diff)
                + " МВт🔻\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
if text_down != "⚡️Разгрузка:\n":
    message_str = message_str + text_down

# Большие отклонения факта
text_fraction = "⚡️Отклонения больше 50%: (План/Факт)\n"
for row_index in range(len(compare_dataframe.index)):
    if (
        compare_dataframe.TotalBR[row_index] > 1
        and compare_dataframe.gtp[row_index] != 0
    ):
        fraction = round(
            compare_dataframe.fact[row_index]
            / compare_dataframe.TotalBR[row_index]
            * 100
        )
        if fraction < 50:
            text_fraction = (
                text_fraction
                + f"{compare_dataframe.GTP_NAME[row_index]} на"
                f" {fraction-100}%🔻"
                f" ({round(compare_dataframe.TotalBR[row_index],2)}/{round(compare_dataframe.fact[row_index],2)})\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
        elif fraction > 150:
            text_fraction = (
                text_fraction
                + f"{compare_dataframe.GTP_NAME[row_index]} на"
                f" {fraction-100}%⬆️ "
                f" ({round(compare_dataframe.TotalBR[row_index],2)}/{round(compare_dataframe.fact[row_index],2)})\n"
            )
            grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
if text_fraction != "⚡️Отклонения больше 50%: (План/Факт)\n":
    message_str = message_str + text_fraction

# Недосбор
text_nodata = "⚡️Недосбор:\n"
for row_index in range(len(compare_dataframe.index)):
    if compare_dataframe.gtp[row_index] == 0:
        text_nodata = (
            text_nodata + str(compare_dataframe.GTP_NAME[row_index]) + "\n"
        )
if text_nodata != "⚡️Недосбор:\n":
    message_str = message_str + text_nodata

# Факт генерации без графика
text_genfact = "⚡️Факт генерации без графика:\n"
for row_index in range(len(compare_dataframe.index)):
    if (
        compare_dataframe.TG[row_index] == 0
        and compare_dataframe.TotalBR[row_index] == 0
        and compare_dataframe.fact[row_index] > 0
    ):
        text_genfact = (
            text_genfact
            + str(compare_dataframe.GTP_NAME[row_index])
            + " "
            + str(compare_dataframe.fact[row_index])
            + " МВт\n"
        )
        grafana_screenshot(str(compare_dataframe.GTP_NAME[row_index]))
if text_genfact != "⚡️Факт генерации без графика:\n":
    message_str = message_str + text_genfact

pictures = getfilenames()

if len(pictures) > 0:
    res_pic = skimage.util.montage(
        skimage.io.imread_collection(pictures), multichannel=True
    )
    skimage.io.imsave(f"{graf_screen}res_pic.png", res_pic)

if message_str != datetime_header and len(pictures) > 0:
    telegram(2, message_str, f"{graf_screen}res_pic.png", datetime_header)
elif message_str != datetime_header and len(pictures) == 0:
    telegram(2, message_str, None, None)
elif message_str == datetime_header:
    telegram(
        2,
        f"{message_str}⚡️Генерация по всем ГТП в штатном режиме.✅",
        None,
        None,
    )


print("Финиш мониторинга отклонений графиков. 🏁")
print("Время выполнения:", str(datetime.datetime.now() - start_time)[0:10])
telegram(
    1,
    "Финиш мониторинга отклонений графиков. 🏁"
    + "  ("
    + " ∆="
    + (str(datetime.datetime.now() - start_time)[0:9])
    + ") \n ID: "
    + id_message,
    None,
    None,
)
browser.quit()
