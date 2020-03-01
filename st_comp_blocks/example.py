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

###################################
# Example of mail sending
###################################

import smtplib
 
 host = "smtp.mail.ru"
 subject = "Test email from Python"
 to_emails = ["bellonin_kirill@mail.ru"]
 from_addr = "bellonin_kirill@mail.ru"
 body_text = "This is my email."

# carbon copy
cc_emails = []
# blind carbon copy
bcc_emails = []

BODY = "\r\n".join((
    "From: %s" % from_addr,
        "To: %s" % ', '.join(to_emails),
            "CC: %s" % ', '.join(cc_emails),
                "BCC: %s" % ', '.join(bcc_emails),
                    "Subject: %s" % subject ,
                        "",
                            body_text
                            ))

try:
    server = smtplib.SMTP_SSL(host=host, port=465, timeout=5) # 5 seconds
        server.login("bellonin_kirill", "theworstthingintheworld")
            server.sendmail(from_addr, to_emails, BODY)
                server.quit()
                except Exception as e:
                    print(e)

###################################
# Example of traceback handling
###################################

import sys
import traceback

try:
    raise ValueError("123123::!")
    except Exception as e:
        print(str(e))
        print("")
        # Get current system exception
        ex_type, ex_value, ex_traceback = sys.exc_info()

        # Extract unformatter stack traces as tuples
        trace_back = traceback.extract_tb(ex_traceback)

        # Format stacktrace
        stack_trace = list()

        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

        print("Exception type : %s " % ex_type.__name__)
        print("Exception message : %s" %ex_value)
        print("Stack trace : %s" %stack_trace)


