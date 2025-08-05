import os, sys
working_dir = os.getcwd()
sys.path.append(working_dir) if working_dir not in sys.path else None

def main():
  if len(sys.argv) > 1 and sys.argv[1] == "model_sales":
    import lib.dx as dx
    import app.model.model_sales as model_sales
    dx.reload(model_sales)
    model_sales.execute(output_dir=model_sales.output_dir_default())
  else:
    raise Exception("usage: python ./src/app.py <model_name>")

if __name__ == "__main__":
  sys.exit(main())
