import json
import io

from . import ComputationalBlock, CBStorage


class TestCB(ComputationalBlock):
    cb_props = set([])
    patch_props = {'hist'}

    def __init__(self, storage):
        super(TestCB, self).__init__(storage)

        self.hist = []
        self.bin = b'123123123'
        return

    def get_json(self, to_str=False):
        _res = super(TestCB, self).get_json(to_str=False)
        _res.update({
            "hist": self.hist
        })
        if to_str:
            return json.dumps(_res)
        return _res

    def get_binary(self):
        stream = io.BytesIO()
        stream.write(self.bin)
        _val = stream.getvalue()
        stream.close()
        return _val

    @classmethod
    def from_json_binary(cls, storage, block_json, block_binary, strict=True, full=True):
        obj = super(TestCB, cls).from_json_binary(storage, block_json, block_binary, strict=strict, full=full)
        obj.bin = block_binary
        return obj

    def calculate(self):
        return None


def test_jupyter():
    from IPython.display import display, HTML
    storage = CBStorage("postgresql://kirbel@82.202.247.23:5060/t0", "test")
    storage.create_storage()
    storage.show()

    tcb = TestCB.load(storage, 1)
    tcb.hist = []
    tcb.save()
    print("TCB")
    display(HTML(tcb._repr_html_()))
    tcb.hist.extend(["1", "1"])
    print("TCB")
    display(HTML(tcb._repr_html_()))
    tcb2 = TestCB.load(storage, 1)
    tcb.push_patch_props()
    print("TCB2")
    display(HTML(tcb2._repr_html_()))
    tcb2.pull_patch_props()
    print("TCB2")
    display(HTML(tcb2._repr_html_()))

    storage.close()
    return
