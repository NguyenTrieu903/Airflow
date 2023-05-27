import snakebite
from airflow.hooks.hdfs_hook import HDFSHook
import datetime
from datetime import timedelta


class HdfsDataset:

    def __init__(self, path, frequency=None, start_date=None, end_date=None, flag="", conn_id="hdfs_default",
                 hook=HDFSHook):
        """

        :param path:
        :param frequency:
        :param start_date:
        :param flag:
        :param conn_id:
        :param hook:
        """
        self.conn_id = conn_id
        self.hook = hook
        self.path = path
        self.frequency = frequency
        self.start_date = start_date
        self.flag = flag
        if end_date is not None:
            self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        else:
            self.end_date = None

    def get_latest(self, n=0):
        """

        :param n: n >= 0
        :return:
        """
        result = []
        sb = self.hook(self.conn_id).get_conn()
        today = self.end_date
        tmp = self.start_date
        d = timedelta(days=self.frequency)
        while tmp <= today:
            lop = [(self.path.format(date=tmp.strftime('%Y%m%d')))]
            try:
                list_of_path_in_hdfs = sb.ls(paths=lop, include_children=False, include_toplevel=True)
                result = result + [x['path'] for x in list_of_path_in_hdfs]
            except snakebite.errors.FileNotFoundException:
                pass
            except Exception as e:
                raise e
            tmp = tmp + d
        result.sort(reverse=True)
        return result[n]

    def get_first(self):
        result = None
        sb = self.hook(self.conn_id).get_conn()
        try:
            childs = self.get_child_paths_of_path()
            for child in childs:
                list_of_child_paths = sb.ls(paths=[child], include_children=True, include_toplevel=False)
                if " ".join([x['path'] for x in list_of_child_paths]).__contains__(self.flag):
                    result = child
                    return result

        except IndexError as e:
            print (e)
            result = None
        except Exception as e:
            print (e)
            result = None
        finally:
            return result

    def get_latest_of_directory(self):
        result = None
        sb = self.hook(self.conn_id).get_conn()
        try:
            childs = self.get_child_paths_of_path()
            for child in reversed(childs):
                list_of_child_paths = sb.ls(paths=[child], include_children=True, include_toplevel=False)
                if " ".join([x['path'] for x in list_of_child_paths]).__contains__(self.flag):
                    result = child
                    return result

        except IndexError as e:
            print (e)
            result = None
        except Exception as e:
            print (e)
            result = None
        finally:
            return result

    def get_child_paths_of_path(self):
        result = None

        def directory_filter(x):
            if x['file_type'] == 'd':
                return True
            return False

        sb = self.hook(self.conn_id).get_conn()
        try:
            list_of_paths = sb.ls(paths=[self.path], include_children=True, include_toplevel=False)
            directories = filter(directory_filter, [x for x in list_of_paths])
            result = [x['path'] for x in directories]
            result.sort()
        except IndexError as e:
            print (e)
            result = None
        except Exception as e:
            print (e)
            result = None
        finally:
            return result

    def get_non_empty_path(self, size, level=1):

        def non_empty_filter(x):
            if x['length'] >= size:
                return True
            return False

        sb = self.hook(self.conn_id).get_conn()
        list_paths = [self.path]
        for i in range(level):
            try:
                non_empty_paths = filter(non_empty_filter, [x for x in sb.du(list_paths)])
            except snakebite.errors.InvalidInputException as e:
                print (e)
                return None
            list_paths = [p['path'] for p in non_empty_paths]

        if len(list_paths) > 0:
            return list_paths[0]
        return None
