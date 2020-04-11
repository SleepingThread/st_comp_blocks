storage = CBStorage("<path to db>", "table_name")
storage.create_storage() # optionally, or in the beginning of usage
comp_block = ComputationalBlock(storage)
# comp_block.test() # if test mode, before you can fill this class with something
comp_block.calculate()
comp_block.save(update=True)

# what is update?:
# update must not use appropriate ComputationalBlock load methods.
cb_update = CBUpdate(storage) # or block_id
cb_update.update(block_id, save=True)
# cb_update.save()