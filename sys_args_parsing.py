import optparse
from collections import OrderedDict

from pyspark import SparkContext
from pyspark.serializers import CloudPickleSerializer


def patch_def(mapCreationProgram):
    print 'inside patch_def'


def nsf_alignment(mapCreationProgram):
    print 'inside nsf_alignment'


def spm_alignment(mapCreationProgram):
    print 'inside spm_alignment'


def local_global(mapCreationProgram):
    print 'inside local_global'


def export(mapCreationProgram):
    print 'inside export'


def clustering_3D(mapCreationProgram):
    print 'inside clustering_3D'


def sd_topography(mapCreationProgram):
    print 'inside sd_topography'


def lane_marks_modelling(mapCreationProgram):
    print 'inside lane_marks_modelling'


def hd_topology(mapCreationProgram):
    print 'inside hd_topology'


def dp(mapCreationProgram):
    print 'inside dp'


def association_to_hd_topology(mapCreationProgram):
    print 'inside association_to_hd_topology'


def semantics(mapCreationProgram):
    print 'inside semantics'


def export_rb(mapCreationProgram):
    print 'inside export_rb'

from ransac.ransac import ransacStep


steps_to_func_dict = OrderedDict([('patch_def', ransacStep), ('nsf_alignment', nsf_alignment), ('spm_alignment', spm_alignment),
                                  ('local_global',local_global), ('export',export), ('clustering_3D',clustering_3D),
                                  ('sd_topography',sd_topography), ('lane_marks_modelling',lane_marks_modelling), ('hd_topology',hd_topology), ('dp',dp),
                                  ('association_to_hd_topology',association_to_hd_topology), ('semantics',semantics), ('export_rb',export_rb)])


def validate_config(config):
    def validate_step():
        if not (step_name in steps_to_func_dict.keys()): raise Exception('Illegal step: {0}'.format(step_name))

    [validate_step for step_name in config['steps_to_run']]


def main(args=None):
    config = load_configurations(args)
    sc = SparkContext('local[*]', 'app', serializer=CloudPickleSerializer())
    MapCreationProgram(config=config,sc=sc)


def load_configurations(args):
    option_parser = optparse.OptionParser()
    option_parser.add_option('-f', dest='config_file', type='str', help='config_file', default="config.json")
    options, _ = option_parser.parse_args(args=args)
    import json
    with open(options.config_file) as f:
        config = json.load(f)
        validate_config(config)
    return config


class MapCreationProgram:
    def __init__(self,config,sc = None):
        self.config = config
        self.sc = sc
        self.__run()

    def __run(self):
        for step_name in steps_to_func_dict:
            if(step_name in self.config['steps_to_run']):
                steps_to_func_dict[step_name](self)


if __name__ == '__main__':
    main()