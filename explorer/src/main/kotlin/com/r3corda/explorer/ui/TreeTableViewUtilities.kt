package com.r3corda.explorer.ui

import com.r3corda.explorer.formatters.Formatter
import javafx.beans.binding.Bindings
import javafx.beans.value.ObservableValue
import javafx.scene.control.TreeTableCell
import javafx.scene.control.TreeTableColumn
import javafx.scene.control.TreeTableView
import javafx.util.Callback
import org.fxmisc.easybind.EasyBind


fun <S> TreeTableView<S>.setColumnPrefWidthPolicy(
        getColumnWidth: (tableWidthWithoutPaddingAndBorder: Number, column: TreeTableColumn<S, *>) -> Number
) {
    val tableWidthWithoutPaddingAndBorder = Bindings.createDoubleBinding({
        val padding = padding
        val borderInsets = border?.insets
        width -
                (if (padding != null) padding.left + padding.right else 0.0) -
                (if (borderInsets != null) borderInsets.left + borderInsets.right else 0.0)
    }, arrayOf(columns, widthProperty(), paddingProperty(), borderProperty()))

    columns.forEach {
        it.setPrefWidthPolicy(tableWidthWithoutPaddingAndBorder, getColumnWidth)
    }
}

private fun <S> TreeTableColumn<S, *>.setPrefWidthPolicy(
        widthWithoutPaddingAndBorder: ObservableValue<Number>,
        getColumnWidth: (tableWidthWithoutPaddingAndBorder: Number, column: TreeTableColumn<S, *>) -> Number
) {
    prefWidthProperty().bind(EasyBind.map(widthWithoutPaddingAndBorder) {
        getColumnWidth(it, this)
    })
}

fun <S, T> Formatter<T>.toTreeTableCellFactory() = Callback<TreeTableColumn<S, T?>, TreeTableCell<S, T?>> {
    object : TreeTableCell<S, T?>() {
        override fun updateItem(value: T?, empty: Boolean) {
            super.updateItem(value, empty)
            text = if (value == null || empty) {
                ""
            } else {
                format(value)
            }
        }
    }
}