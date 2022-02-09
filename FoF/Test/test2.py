a = """alt-bg
alt-rcam
arc-nut-wld
bd-bc-qc
bg-assy
bt-wld
e75-nut-wld
e75-wld
fg-bc-qc
isra
mlb-nut-wld
mlb-nut-wld2
oqc-out
ort-final
ort-raw
rcam-assy
sp-wld
spk-nut-wld
ump-t1
ump-t3
volumax
"""
b = a.replace("\n", ",").split(',')
print(b)
dic = {}
for i in b:
    p = i.split('-')
    st = [s.capitalize() for s in p]
    j = "".join(st)
    dic[i] = "TraceResultAll"
# print(dic)

a = ['sn', 'project', 'process', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
               'fixtureid', 'logcreated', 'hiscreated', 'logevent', 'stationid', 'defects', 'uut_stop', 'uut_start',
               'test_result', 'unit_serial_number', 'line_id', 'fixture_id', 'station_id', 'software_name',
               'software_version']
b = ' String,'.join(a)
sql = f"""create table aaaa({b}) engine = MergeTree ORDER BY id;"""
# print(sql)

c = ['id', 'project', 'process', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime', 'fixtureid',
     'logcreated', 'hiscreated', 'logevent', 'hisevent', 'stationid', 'defects', 'sp', 'STATION_STRING', 'uut_stop',
     'uut_start', 'test_result', 'unit_serial_number', 'line_id', 'fixture_id', 'station_id', 'software_name',
     'software_version', 'band',
     'bg_splits_grade', 'head_id', 'mic2_grade', 'rcam_grade', 'cp_tank_id', 'ano_tank_id', 'cp_end_time',
     'dye_tank_id', 'ano_end_time', 'dye_end_time', 'seal_tank_id', 'cp_start_time', 'seal_end_time', 'ano_start_time',
     'dye_start_time',
     'seal_start_time', 'band_sn', 'sandblasting_start_time', 'rework_item', 'rework_label', 'weld_stop_time',
     'weld_start_time', 'Build', 'Color', 'color', 'band_full_sn', 'resin_lot_batch_id', 'bg',
     'full_sn_bg', 'full_sn_band', 'assy_stop_time', 'assy_start_time', 'bg_glue_cavity_id', 'bg_glue_stop_time',
     'bg_glue_fixture_id', 'bg_glue_start_time', 'bg_glue_station_id', 'bg_glue_software_version', 'tossing_item',
     'Region', 'properties', 'DOE', 'S_BUILD', 'doe_item', 'config_name', 'scrap_reason', 'full_sn_top_u',
     'full_sn_bottom_u', 'full_sn_left_rail', 'full_sn_right_rail', 'paper_tracking_top_u', 'paper_tracking_bottom_u',
     'paper_tracking_left_rail', 'paper_tracking_right_rail', 'data', 'fg', 'full_sn_fg', 'full_sn_sp',
     'full_sn', 'operator', 'cavity_id', 'machine_id', 'process_rev', 'fg_sn', 'retest', 'operator_id', 'sample_type',
     'failure_mode', 'max_shifting', 'project_name', 'failure_remark', 'ort_test_number', 'parameter_value',
     'shift_max_force', 'failure_location', 'lower_limit_ipqc', 'upper_limit_ipqc', 'control_limit_ipqc',
     'loading_breakpoint', 'ort_test_stop_time', 'ort_test_start_time', 'calibration_status',
     'calibration_expiration_date', 'glue_stop_time',
     'glue_start_time',
     'precitec_rev', 'fixtureB_id', 'fixtureC_id', 'GRR', 'bin', 'user', 'RECIPE', 'DEGREES_C', 'HSG_COLOR',
     'build_event',
     'SIM_PROFILEID', 'cnc4_dot_matrix', 'cnc5_1_dot_matrix', 'cnc5_2_dot_matrix', 'bg_sn', 'sp_sn',
     'CG_OPENING_LW_BIN', 'FAI_240_Offset_Bin',
     'FAI_173_Volplus_Offset_Bin', 'FAI_173_Volminus_Offset_Bin']
