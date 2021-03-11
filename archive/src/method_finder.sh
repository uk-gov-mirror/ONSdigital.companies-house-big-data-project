echo "===self.method() calls==="
self_method_list=$(grep -r --include \*.py -E 'self\.[a-zA-Z0-9_\.]+\(' | grep -Ev 'self\.fs|self\.__init__')
echo "$self_method_list"

echo "===Method Headers==="
method_headers=$(grep -r --include \*.py 'def ')
echo "$method_headers"

echo "===Classes==="
class_headers=$(grep -r --include \*.py -E 'class ' | grep -Ev '"""' | grep -E ':$')
echo "$class_headers"

echo "===Class names==="
class_names=$(grep -r --include \*.py -E 'class ' | grep -Ev '"""' | grep -E ':$' | awk '{print $2}' | awk -F '(' '{print $1}' | sed 's/://g')
echo "$class_names"

class_names=($class_names)

# ====================================

echo "===Class static method calls==="
for c in ${class_names[@]}; do

    echo '---'$c'---'
    instances=$(grep -rh --include \*.py -E "$c"'\.[a-zA-Z0-9_\.]+\(' | sed 's/^[ \t]*//g' | grep -Ev 'import|^#' | awk -F " = " '{if (NF==1) print $1; else print $2;}')
    echo "$instances"
done

# ================================

echo "===Class instance method calls==="
for c in ${class_names[@]}; do

    echo '---'$c'---'
    instances=$(grep -rh --include \*.py ' '"$c"'(' | sed 's/^[ \t]*//g' | grep -Ev 'import|^#|^class|^return|^yield' | awk -F "=" '{print $1}')

    echo 'instances: '"$instances"

    instances=($instances)
    for i in ${instances[@]}; do

        echo '...'$i'...'
        instance_calls=$(grep -r --include \*.py -E "$i"'\.[a-zA-Z0-9_\.]+\(' | sed 's/^[ \t]*//g' | grep -Ev '^#' )
        echo "$instance_calls"
    done
done