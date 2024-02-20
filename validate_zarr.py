#!/usr/bin/env python3

import sys
import zarr
import fsspec
import time
import os.path

def validate_dataset(dataset):
    errors = 0

    def log(message, indent=0):
        print(f"{'  ' * indent}{message}")

    # Basic Structure Check: List all items at the root level (groups and arrays)
    log("Root items:", 1)
    log(f"{list(dataset.keys())}", 2)

    # Iterate through each item to perform further checks
    for name, item in dataset.items():
        if isinstance(item, zarr.hierarchy.Group):
            log(f"Group '{name}' found with items: {list(item.keys())}", 2)
            # Recursive check can be performed here for nested groups
        elif isinstance(item, zarr.core.Array):
            log(f"Array '{name}' found with shape {item.shape} and dtype {item.dtype}", 2)
            # Data Integrity Check: Attempt to read a small slice
# expensive!
#            try:
#                _ = item[0]
#                log(f"Array '{name}' is accessible.", 3)
#            except Exception as e:
#                log(f"Error accessing array '{name}': {e}", 3)
#                errors += 1
        else:
            log(f"Unknown item '{name}' found.", 2)
            errors += 1

    # Metadata Validation (example for the root group)
    try:
        attrs = dataset.attrs.asdict()
        log("Root attributes:", 2)
        log(f"{attrs}", 3)
        # Here you can add checks against expected attributes
    except Exception as e:
        log(f"Error reading root attributes: {e}", 2)
        errors += 1

    return errors


def estimate_zarr_size(dataset):
    num_files = 0
    total_size = 0

    def estimate_array_size(array):
        # Estimate the size of an array based on its dtype and shape
        itemsize = array.dtype.itemsize
        num_elements = array.size
        return itemsize * num_elements

    def traverse(node):
        nonlocal num_files, total_size
        if isinstance(node, zarr.core.Array):
            num_files += 1  # Counting the array metadata file
            num_files += node.nchunks  # Add the number of chunk files
            total_size += estimate_array_size(node)  # Approximate size from metadata
        elif isinstance(node, zarr.hierarchy.Group):
            num_files += 1  # Counting the group metadata file
            for child in node.values():
                traverse(child)  # Recursively traverse the group

    traverse(dataset)
    return num_files, total_size


# Check if a URL was provided as a command line argument
if len(sys.argv) < 2:
    print("Usage: python script.py <zarr_dataset_url>")
    sys.exit(1)

# Start timing
start_time = time.time()

# The first command line argument is the script name, so the URL is the second
zarr_url = sys.argv[1]

# Open the Zarr dataset directly from the URL
# hangs???
# store = fsspec.get_mapper(zarr_url)
store = fsspec.get_mapper('simplecache::' + zarr_url, simplecache={'cache_storage': os.path.expanduser('~/.cache/zarr_cache')})

# End timing
elapsed_time1 = elapsed_time2 = -100
num_files = total_size = "N/A"

# Open the Zarr dataset
try:
    dataset = zarr.open_consolidated(store=store)

    # Validate the dataset
    errors = validate_dataset(dataset)
    # End timing
    end_time = time.time()
    elapsed_time1 = end_time - start_time

    # Assuming `dataset` is your Zarr dataset object
    num_files, total_size = estimate_zarr_size(dataset)
    elapsed_time2 = time.time() - end_time
except Exception:
    errors = "failed-to-open"
    import traceback
    traceback.print_exc()

# Print summary
print(f"url: {zarr_url} #errors: {errors} time: {elapsed_time1:.2f} seconds  num_files: {num_files}  total_size est: {total_size}  time: {elapsed_time2:.2f}")

