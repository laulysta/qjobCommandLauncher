import re
import numpy as np
import numpy.random as npr
from collections import OrderedDict


def build_argument_templates_dictionnary():
    # Order matter, if some regex is more greedy than another, the it should go after
    argument_templates = OrderedDict()
    argument_templates[RangeArgumentTemplate.__name__] = RangeArgumentTemplate()
    argument_templates[ListArgumentTemplate.__name__] = ListArgumentTemplate()
    return argument_templates


class ArgumentTemplate(object):
    def __init__(self):
        self.regex = ""

    def unfold(self, match):
        raise NotImplementedError("Subclass must implement method `unfold(self, match)`!")


class ListArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "\[[^]]*\]"

    def unfold(self, match):
        return match[1:-1].split(' ')


class RangeArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "\[(\d+):(\d+)(?::(\d+))?\]"

    def unfold(self, match):
        groups = re.search(self.regex, match).groups()
        start = int(groups[0])
        end = int(groups[1])
        step = 1 if groups[2] is None else int(groups[2])
        return map(str, range(start, end, step))


class LinearArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "(Lin)\[(-?\d+)\,(-?\d+)\,(\d+)\]"

    def unfold(self, match):
        groups = re.search(regex, match).groups()
        start, stop, npoints = [int(el) for el in groups[1:]]
        return map(str, np.linspace(start, stop, npoints))


class LogArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "(Log)\[(-?\d+)\,(-?\d+)\,(\d+)(?:,(\d+))?\]"

    def unfold(self, match):
        groups = re.search(regex, match).groups()
        start, stop, npoints = [int(el) for el in groups[1:-1]]
        base = 10 if groups[-1] is None else int(groups[-1])
        return map(str, np.logspace(start, stop, npoints, base=base))


class UniformArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "(U)\[(-?\d+)\,(-?\d+),(-?\d+)\]"

    def unfold(self, match):
        groups = re.search(self.regex, match).groups()
        low, high, nsamples = [int(el) for el in groups[1:]]
        return map(str, npr.uniform(low, high, size=(nsamples, )))


class NormalArgumentTemplate(ArgumentTemplate):
    def __init__(self):
        self.regex = "(N)\[(-?\d+)\,(\d+),(-?\d+)\]"

    def unfold(self, match):
        groups = re.search(self.regex, match).groups()
        loc, scale, nsamples = [int(el) for el in groups[1:]]
        return map(str, npr.normal(loc, scale, size=(nsamples, )))

argument_templates = build_argument_templates_dictionnary()
