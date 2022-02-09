#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: MQtest.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 13, 2021


"""多进程"""
import datetime
import gc
import json
from multiprocessing import cpu_count, Pool
import pandas as pd
import sqlalchemy
from tqdm import tqdm


def insertSQl(data1, begin, end):
    print(begin)
    print(end)
    inData = data1.iloc[begin:end, 0:]
    try:
        insertIntoSql(inData, "testmq")
        # inData.to_sql("testmq", engine, index=False, if_exists='append')
        gc.collect()  # 回收内存空间
    except Exception as e:
        print(e)

        # for i in tqdm(range(begin, end)):  # python进度条 tqdm

        # if i % 100 == 0:  # 每10000次进行一次插入，速度没有经过校验，不确定10000万次是否为最佳
        #     print(end)
        #     inData = data1.iloc[begin:end, 0:]
        #     try:
        #         insertIntoSql(inData, "testmq")
        #         gc.collect()  # 回收内存空间
        #     except Exception as e:
        #         print(e)

        """
        此处有个变量拼接，除余不满一万次时会继续拼接变量
        """


def convertToPandas(body):  # 字典转为DateFrame
    pd.set_option('display.max_columns', None)  # 显示所有列

    # 展平数据
    pdData = pd.json_normalize(
        body,
        # record_path=['data'],   # 展开json内列表
        # meta=['id', 'agent', 'event', 'created', 'serials', 'defects',
        # 'project', 'process', ['data', 'insight']]  # 展开json内列表加字典
    )
    pdColumns = pdData.keys()
    columns = [i.split(".")[-1] for i in pdColumns]  # 拆分列名
    pdData.columns = columns
    pdData = pdData[
        ['id', 'agent', 'event', 'created', 'project', 'band_sn', 'pu_stop_time', 'tossing_item', 'pu_start_time',
         'STATION_STRING', 'paint_batch_sn', 'additive_batch_sn', 'hardener_batch_sn', 'uv_pigment_batch_sn',
         'uut_stop', 'uut_start', 'test_result', 'unit_serial_number', 'head_id', 'line_id', 'fixture_id', 'station_id',
         'software_name', 'software_version', 'band', 'process', 'fg_sn', 'fg', 'polishing_end_time',
         'post_polishing_start_time', 'post_wet_sanding_cleaning_stop_time', 'sp_sn', 'location1', 'location2',
         'location3', 'location4', 'location5', 'location6', 'location7', 'location8', 'fixture_id', 'precitec_rev',
         'weld_stop_time', 'weld_start_time', 'precitec_grading', 'location1_layer1_pulse_profile',
         'location2_layer1_pulse_profile', 'location3_layer1_pulse_profile', 'location4_layer1_pulse_profile', 'sp',
         'color', 'band_full_sn', 'Site', 'Color', 'location9', 'location10', 'location11', 'location12', 'location13',
         'location14', 'location15', 'location16', 'location17', 'location18', 'location19', 'location20', 'location21',
         'location22', 'location23', 'location24', 'location25', 'location26', 'location27', 'location28', 'location29',
         'location30', 'location31', 'location32', 'depu_stop_time', 'depu_start_time', 'rework_item', 'rework_label',
         'ir_texturing_stop_time', 'ir_texturing_start_time', 'cavity_id', 'dispense_stop_time', 'dispense_start_time',
         'de_pvd_stop_time', 'laser_machine_id', 'de_pvd_start_time', 'actual_power_judgment',
         'actual_power_measure_time', 'GRR', 'bin', 'user', 'RECIPE', 'full_sn', 'DEGREES_C', 'HSG_COLOR', 'RINGER_BIN',
         'build_event', 'SIM_PROFILEID', 'cnc5_dot_matrix', 'cnc6_dot_matrix', 'cnc7_dot_matrix', 'cnc4_1_dot_matrix',
         'side_fire_sn', 'glue_stop_time', 'glue_start_time', 'glue_sw_version', 'inspection_sw_version',
         'side_fire_glue_batch_sn', 'side_fire_glue_batch_SN_scan_in', 'laser_pulse_profile_measure_setting',
         'assy_stop_time', 'assy_start_time', 'COLOR_BIN', 'im_cavity_id', 'cnc4_dot_matrix', 'cnc5_1_dot_matrix',
         'cnc5_2_dot_matrix', 'im_tooling_number', 'bg_sn', 'config_name', 'box_sn', 'building', 'quarantine_item',
         'bg', 'EMT', 'mic2_glue_batch_sn', 'mic2_glue_batch_SN_scan_in', 'okos_bin', 'im1_cavity_id', 'im2_cavity_id',
         'CG_OPENING_LW_BIN', 'rcam_brace_vendor', 'im1_tooling_number', 'im2_tooling_number', 'rcam_brace_cavity_id',
         'rcam_brace_tooling_number', 'auto_uvpu_color', 'mic2_grade', 'rcam_grade', 'error_1', 'error_2',
         'outer_trimML_vendor', 'outer_trimWI_vendor', 'outer_trimML_dwg_rev', 'outer_trimWI_dwg_rev',
         'inner_trimML_assy_vendor', 'inner_trimWI_assy_vendor', 'inner_trimML_assy_dwg_rev',
         'inner_trimWI_assy_dwg_rev', 'brace_tray_sn', 'assy_bond_time', 'glue_cavity_id', 'glue_machine_id',
         'assy_bond_end_time', 'brace_glue_batch_sn', 'perimeter_glue_plant', 'perimeter_glue_expire_date',
         'brace_glue_batch_SN_scan_in', 'perimeter_glue_batch_number', 'top_u_sn', 'bottom_u_sn', 'left_rail_sn',
         'right_rail_sn', 'top_u_corner_sn', 'bottom_u_corner_sn', 'error_x', 'plasma_stop_time', 'plasma_start_time',
         'data', 'bg_splits_grade', 'lane_id', 'rcamBE_result', 'rcamDB_result', 'rcamPT_result', 'roi_version',
         'parameter_version', 'acquisition_version', 'perimeter_glue_batch_sn', 'perimeter_glue_cavity_id',
         'perimeter_glue_station_id', 'perimeter_glue_batch_SN_scan_in', 'inner_glue_batch_sn', 'inner_glue_cavity_id',
         'inner_glue_station_id', 'inner_glue_batch_SN_scan_in', 'rcam_trim_bin', 'rcam_hole_bin_best_fit',
         'rcam_hole_bin_diagonal', 'rcam_hole_bin_max_inscribed', 'location5_layer1_pulse_profile',
         'location6_layer1_pulse_profile', 'location7_layer1_pulse_profile', 'location8_layer1_pulse_profile',
         'location9_layer1_pulse_profile', 'location10_layer1_pulse_profile', 'pvd_batch_id', 'pvd_stop_time',
         'pvd_chamber_id', 'pvd_start_time', 'pvd_rework_count', 'laser_split_stop_time', 'laser_split_start_time',
         'machine_id', 'sandblasting_start_time', 'spk_bracket_glue_plant', 'spk_bracket_glue_batch_sn',
         'spk_bracket_glue_expire_date', 'spk_bracket_glue_batch_number', 'spk_bracket_glue_batch_SN_scan_in',
         'outer_trimBE_vendor', 'outer_trimDB_vendor', 'outer_trimPT_vendor', 'outer_trimBE_dwg_rev',
         'outer_trimDB_dwg_rev', 'outer_trimPT_dwg_rev', 'inner_trimBE_assy_vendor', 'inner_trimDB_assy_vendor',
         'inner_trimPT_assy_vendor', 'inner_trimBE_assy_dwg_rev', 'inner_trimDB_assy_dwg_rev',
         'inner_trimPT_assy_dwg_rev', 'operator', 'process_rev', 'retest', 'operator_id', 'sample_type', 'failure_mode',
         'max_shifting', 'project_name', 'failure_remark', 'ort_test_number', 'parameter_value', 'shift_max_force',
         'failure_location', 'lower_limit_ipqc', 'upper_limit_ipqc', 'control_limit_ipqc', 'loading_breakpoint',
         'ort_test_stop_time', 'ort_test_start_time', 'calibration_status', 'calibration_expiration_date',
         'rcamB_grade', 'rcamT_grade', 'side_fire_grade', 'spkA_grade', 'spkB_grade', 'fixtureB_id', 'fixtureC_id',
         'rcamBE_grade', 'rcamDB_grade', 'rcamPT_grade', 'rack_id', 'basket_id', 'chamber_id', 'vi_stop_time',
         'vi_start_time', 'resin_lot_batch_id', 'Build', 'Sidefire', 'left_rail_coil_no', 'left_rail_heat_no',
         'top_u_cnc1_vendor', 'top_u_forging_lot', 'top_u_slab_number', 'right_rail_coil_no', 'right_rail_heat_no',
         'bottom_u_cnc1_vendor', 'bottom_u_forging_lot', 'bottom_u_slab_number', 'top_u_forging_vendor',
         'left_rail_cnc1_vendor', 'right_rail_cnc1_vendor', 'bottom_u_forging_vendor', 'top_u_corner_cnc1_vendor',
         'top_u_corner_forging_lot', 'top_u_corner_slab_number', 'left_rail_profile_bar_lot',
         'top_u_raw_material_vendor', 'right_rail_profile_bar_lot', 'bottom_u_corner_cnc1_vendor',
         'bottom_u_corner_forging_lot', 'bottom_u_corner_slab_number', 'top_u_corner_forging_vendor',
         'bottom_u_raw_material_vendor', 'left_rail_profile_bar_vendor', 'left_rail_raw_material_vendor',
         'right_rail_profile_bar_vendor', 'bottom_u_corner_forging_vendor', 'right_rail_raw_material_vendor',
         'top_u_corner_raw_material_vendor', 'bottom_u_corner_raw_material_vendor', 'full_sn_bg', 'full_sn_band',
         'bg_glue_head_id', 'point1_glue_edge', 'point2_glue_edge', 'point3_glue_edge', 'point4_glue_edge',
         'point5_glue_edge', 'point6_glue_edge', 'point7_glue_edge', 'point8_glue_edge', 'point9_glue_edge',
         'bg_glue_stop_time', 'point10_glue_edge', 'point11_glue_edge', 'point12_glue_edge', 'point13_glue_edge',
         'point14_glue_edge', 'point15_glue_edge', 'point16_glue_edge', 'point1_glue_width', 'point2_glue_width',
         'point3_glue_width', 'point4_glue_width', 'point5_glue_width', 'point6_glue_width', 'point7_glue_width',
         'point8_glue_width', 'point9_glue_width', 'bg_glue_fixture_id', 'bg_glue_start_time', 'bg_glue_station_id',
         'point10_glue_width', 'point11_glue_width', 'point12_glue_width', 'point13_glue_width', 'point14_glue_width',
         'point15_glue_width', 'point16_glue_width', 'bg_glue_software_version', 'Region', 'full_sn_top_u',
         'full_sn_left_u', 'full_sn_right_u', 'full_sn_bottom_u']]

    print('转换数据成功')
    print('-----------')
    return pdData


def insertIntoSql(frame, tableName2):
    # DataFrame数据插入PG sql
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    # connection = engine.raw_connection()
    print('链接PG SQL成功')
    # print(frame)
    frame.to_sql(tableName2, engine, index=False, if_exists='append', method='multi', index_label=None,
                 chunksize=None, )  # method='multi'在单个INSERT子句中传递多个值,提高速度
    engine.dispose()
    print('数据插入PG SQL成功')


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    pd.set_option('display.max_columns', None)  # 显示所有列
    jsonPath = 'readFromMq.json'
    # 使用 Python JSON 模块载入数据
    with open(jsonPath, 'r') as f:
        data = json.loads(f.read())

    jsonData = convertToPandas(data)
    p = Pool(cpu_count())  # cpu_count 查询当前设备进程数，我的是八核，所以下面分了8个进程
    p.apply_async(insertSQl, args=[jsonData, 0, 100])
    p.apply_async(insertSQl, args=[jsonData, 101, 200])
    p.apply_async(insertSQl, args=[jsonData, 201, 300])
    p.apply_async(insertSQl, args=[jsonData, 301, 400])
    p.apply_async(insertSQl, args=[jsonData, 401, 500])
    p.apply_async(insertSQl, args=[jsonData, 501, 600])
    p.apply_async(insertSQl, args=[jsonData, 601, 700])
    p.apply_async(insertSQl, args=[jsonData, 701, 800])
    p.close()
    p.join()

    endTime = datetime.datetime.now()
    print(endTime - startTime)
