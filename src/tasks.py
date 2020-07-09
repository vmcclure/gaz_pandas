import pandas as pd
import  xlrd
import datetime
import time

def time_decorator(function):
    def wrapper(*args, **kwargs):
        now = time.time()
        print(f'-- {function.__name__} стартовала')
        function()
        print(f'-- {function.__name__} завершилась за {time.time()-now}\n')
    return wrapper


@time_decorator
def task1():
    # '''Необходимо для резервуара КЕМ11 Нефтебазы К реализовать следующие задачи:
    # Загрузить данные целиком, отфильтровав по необходимому резервуару нефтебазы. Период выбрать
    #  01.06.2020 - 03.06.2020 на основе поля shiftendt из таблицы shifts_data.
    # '''
    data = pd.read_excel('shifts_data.xlsx', parse_dates=['shiftbegt', 'shiftendt']).drop_duplicates()
    data = data[(data['objectid'] == "КЕМ11") & (data["shiftendt"] > datetime.datetime(year=2020, month=6, day=1)) &
                (data["shiftendt"] < datetime.datetime(year=2020, month=6, day=3))]
    return data


@time_decorator
def task2():
    # '''Необходимо предобработать входные данные. В таблицах присутствует история корректировки и
    #  перевыгрузки данных по каждой смене. За это отвечает поле version в каждой таблице. В анализе должны
    #   присутствовать только актуальные записи по сменам. Соответственно, получить их можно, отобрав в таблице
    #   shifts_data записи с максимальным значением версии (version) по каждой смене (shiftnumber).'''
    data = pd.read_excel('shifts_data.xlsx', parse_dates=['shiftbegt', 'shiftendt']).drop_duplicates()

    idx = data.groupby(['shiftnumber'])['version'].transform(max) == data['version']
    data = data[idx]
    return data


@time_decorator
def task3():
    # '''3.	Подготовить таблицу Отгрузка из резервуара. Данная таблица должна содержать в себе только операции типа ОТГРУЗКА. Состав полей следующий:
    # a.	Номер смены
    # b.	Дата и время начала проведения операции
    # c.	Дата и время окончания проведения операции
    # d.	Номер документа (201 атрибут операции)
    # e.	Нефтепродукт
    # f.	Паспорт качества (203 атрибут операции)
    # g.	Госномер бензовоза (214 атрибут операции)
    # h.	Номер секции в цистерне бензовоза (202 атрибут операции)
    # i.	Масса нефтепродукта по документу (207 атрибут операции)
    # j.	Версия пакета выгрузки по рабочей смене (version)
    # '''
    operations_view = pd.read_excel('operations_view.xlsx', parse_dates=['endtime']).drop_duplicates()
    operatons_attrs = pd.read_excel('operations_attrs.xlsx').drop_duplicates()

    operatons_attrs_filtred = operatons_attrs[
                ((operatons_attrs['idattr'] == 201) | (operatons_attrs['idattr'] == 203) |
                 (operatons_attrs['idattr'] == 214) | (operatons_attrs['idattr'] == 202) |
                 (operatons_attrs['idattr'] == 207)) & (operatons_attrs['operation_type'] =='ОТГРУЗКА')]

    result = operatons_attrs_filtred.pivot_table(index='id', columns='idattr', values='valuestr', aggfunc='sum').merge(
                operations_view, on=["id"]).drop(['id','region','objectid','operation_type'],axis='columns')
    result.to_csv('task3.csv',sep=';')


@time_decorator
def task4():
    # 4.	Подготовить таблицу Прием в резервуар. Данная таблица должна содержать в себе только операции типа ПРИЕМ. Состав полей следующий:
    # a.	Номер смены
    # b.	Дата и время начала проведения операции
    # c.	Дата и время окончания проведения операции
    # d.	Номер документа (201 атрибут операции)
    # e.	Нефтепродукт
    # f.	Паспорт качества (203 атрибут операции)
    # g.	Номер ЖД цистерны (206 атрибут операции)
    # h.	Масса нефтепродукта по документу (207 атрибут операции)
    # i.	Версия пакета выгрузки по рабочей смене (version)

    operations_view = pd.read_excel('operations_view.xlsx', parse_dates=['endtime']).drop_duplicates()
    operatons_attrs = pd.read_excel('operations_attrs.xlsx').drop_duplicates()

    operatons_attrs_filtred = operatons_attrs[
                        ((operatons_attrs['idattr'] == 201) | (operatons_attrs['idattr'] == 203) |
                        (operatons_attrs['idattr'] == 206 ) | (operatons_attrs['idattr'] == 207)) &
                        (operatons_attrs['operation_type'] == 'ПРИЕМ')]

    result = operatons_attrs_filtred.pivot_table(index='id', columns='idattr', values='valuestr', aggfunc='sum').merge(
        operations_view, on=["id"]).drop(['id','region','objectid','operation_type'],axis='columns')
    result.to_csv('task4.csv', sep=';')


@time_decorator
def task5():
    # 5.	Подготовить таблицу Итого по сменам. Данная таблица собирает в себе суммарную информацию по смене (группировка по shiftnumber).Состав полей следующий:
    # a.	Номер смены
    # b.	Дата и время начала рабочей смены
    # c.	Дата и время окончания рабочей смены
    # d.	Масса на начало смены (231 атрибут данных по смене)
    # e.	Масса на конец смены (233 атрибут данных по смене)
    # f.	Суммарная масса принятого нефтепродукта
    # g.	Суммарная масса отгруженного нефтепродукта
    # h.	Посчитать отклонение: Масса на начало смены + Суммарная масса отгруженного нефтепродукта - Суммарная масса принятого нефтепродукта - Масса на конец смены
    # i.	Версия пакета выгрузки по рабочей смене (version)
    operations_attrs = pd.read_excel('operations_attrs.xlsx').drop_duplicates()
    shifts_data = pd.read_excel('shifts_data.xlsx', parse_dates=['shiftbegt', 'shiftendt']).drop_duplicates()
    operations_view = pd.read_excel('operations_view.xlsx', parse_dates=['endtime']).drop_duplicates()

    ready_shift_data=shifts_data.pivot_table(index=['shiftnumber'],
                                   columns='attrid', values='attrval')
    clear_shifts_data = shifts_data.drop(['attrid', 'attrval','region', 'objectid','productid'],
                                         axis='columns').drop_duplicates()
    ready_shift_data = ready_shift_data.merge(clear_shifts_data, on='shiftnumber')
    ready_operations_attrs = operations_attrs[operations_attrs['idattr'] == 207]
    attrs_and_view = ready_operations_attrs.merge(operations_view, on=["id", 'operation_type'])
    attrs_and_view['valuestr'] = attrs_and_view['valuestr'].astype('int64')
    df = attrs_and_view.groupby(["shiftnumber",'operation_type'])['valuestr'].agg(valsum = 'sum')
    df = df.pivot_table(index='shiftnumber', columns='operation_type', values='valsum', aggfunc='sum')
    df = df.merge(ready_shift_data, on ='shiftnumber')
    df['отклонение'] = df[231] + df[233] - df['ОТГРУЗКА'] - df['ПРИЕМ'].fillna(0)
    df.to_csv('task5.csv', sep=';')