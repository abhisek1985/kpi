from io import TextIOWrapper
from six import with_metaclass, callable
from abc import ABCMeta, abstractmethod
from json import loads, dumps


class Wrapper(with_metaclass(ABCMeta)):

    def __init__(self, klass=None, override=None):
        self.overridden = False
        self.override_class = override
        if isinstance(self.override_class, TextIOWrapper):
            self.overridden = True

        if klass not in ['JSON', 'CSV', 'EXCEL', 'Dict']:
            raise NotImplementedError('Output Buffer class is unsupported!!')

    @abstractmethod
    def wrap(self, functor, **kwargs):
        """
        The main wrapper class to wrap
        the api functors and pass it along.
        The functors can be wrapped in any
        format however various class can implement
        the functor wrapper in various ways. This is
        an abstract method that is overridden by applied
        wrapper classes

        :param functor: The main function pointer
        """
        pass


class JSONWrap(Wrapper):
    def __init__(self, override=None):
        super(self.__class__, self).__init__(klass='JSON', override=override)

    def wrap(self, functor, **kwargs):
        """
        The wapper class for JSON output.

        :param functor: returns the pandas dataframe
        :param validate: For json validation (default = True)
        :param indent: For overriden wrapper (default = 4)
        """
        if not callable(functor):
            raise AttributeError('No functor is passed!')

        validate = kwargs.get('validate', True)  # JSON validation
        data = functor().to_json()
        if validate:
            self._validate_json(data)
        if self.overridden:
            indent = kwargs.get('indent', 4)  # pprint indentation
            if self.override_class.name != '<stdout>':
                pth = kwargs.get('path', '.')
            print(
                dumps(
                    loads(data),
                    indent=indent,
                    sort_keys=True),
                file=self.override_class)
            return None
        return data

    def _validate_json(self, data):
        """
        This shall take json data and validates
        if the json in syntatically correct or not.

        :param data: Json data
        :return None
        """
        try:
            loads(data)
        except ValueError as e:
            raise e


class DictWrap(Wrapper):
    def __init__(self, override=None):
        super(self.__class__, self).__init__(klass='Dict', override=override)

    def wrap(self, functor, **kwargs):
        """
        The wapper class for JSON output.

        :param functor: returns the pandas dataframe
        """
        if not callable(functor):
            raise AttributeError('No functor is passed!')
        return dumps(functor())


class CSVWrap(Wrapper):

    def wrap(self, functor, **kwargs):
        if not callable(functor):
            raise AttributeError('No functor is passed!')
    pass


class EXCELWrap(Wrapper):

    def wrap(self, functor, **kwargs):
        if not callable(functor):
            raise AttributeError('No functor is passed!')
    pass
