import re
import six

FORMAT_PATTERN = re.compile('{{([^(}})]*)}}')


class Formatter(object):

    def __init__(self, template):
        self.template = six.text_type(template)
        self.refs = []
        self.replacements = {}
        for ref in FORMAT_PATTERN.findall(self.template):
            self.refs.append(ref)
            repl = '{{%s}}' % ref
            self.replacements[repl] = ref

    def apply(self, record):
        value = six.text_type(self.template)
        for repl, ref in self.replacements.items():
            value = value.replace(repl, record.get(ref))
        return value
